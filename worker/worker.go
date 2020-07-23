package worker

import (
	"boson-adapter/host"
	"boson-adapter/host/invocationStatus"
	"boson-adapter/http"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

type EventHandler func(event host.Event) (host.InvocationResult, error)

type EventRegistration struct {
	EventProviderID      string
	EventProviderVersion string
	SubscriberID         string
	Config               map[string]string
}

type Config struct {
	ApplicationID      string
	ApplicationVersion string
	WorkerInstanceID   string
	HostURL            string
	EventRegistrations []EventRegistration
	Handler            EventHandler
	Concurrency        int
	Logger             *log.Logger
}

type Worker struct {
	config Config
	client host.Client
}

func NewWorker(config Config) (*Worker, error) {
	client, err := http.NewHTTPHostClient(config.HostURL)
	if err != nil {
		return nil, err
	}
	return &Worker{
		config: config,
		client: client,
	}, nil
}

func (worker Worker) Start(stopChan chan struct{}) error {

	client, log, workerID := worker.client, worker.config.Logger, worker.config.WorkerInstanceID

	connectResponse, err := client.Connect(host.ConnectRequest{
		ApplicationID:      worker.config.ApplicationID,
		ApplicationVersion: worker.config.ApplicationVersion,
		WorkerInstanceID:   workerID,
	})
	if err != nil {
		return err
	}
	log.Printf("Connect response: %+v\n", connectResponse)

	defer func() {
		log.Println("Disconnecting...")
		err = client.Disconnect(host.DisconnectRequest{WorkerInstanceID: workerID})
		if err != nil {
			log.Println("Error on disconnect", err)
		}
	}()

	eventRegistrations := eventRegistrationsFromFunctionMappings(worker.config.EventRegistrations)

	eventRegistrationsResponse, err := client.Register(host.EventRegistrationsRequest{
		WorkerInstanceID:   workerID,
		EventRegistrations: eventRegistrations},
	)
	if err != nil {
		return err
	}
	log.Printf("Event registrations response: %+v\n", eventRegistrationsResponse)

	defer func() {
		log.Println("Unregistering...")
		err = client.Unregister(host.UnregisterRequest{WorkerInstanceID: workerID})
		if err != nil {
			log.Println("Error on unregister", err)
		}
	}()

	err = errorFromEventRegistrationResponse(&eventRegistrationsResponse)
	if err != nil {
		return err
	}

	heartbeatStopHandle := worker.startHeartbeat(time.Duration(float32(time.Second*time.Duration(connectResponse.WorkerHearbeatTimeoutInSeconds)) * 0.75))
	defer heartbeatStopHandle.stop()

	pollingStopHandle := worker.startHandleEvents(worker.config.Handler)
	defer pollingStopHandle.stop()

	<-stopChan
	close(stopChan)
	log.Println("Got stop signal...")

	return nil
}

type stopHandle struct {
	requestStop chan struct{}
	hasStopped  chan struct{}
}

func newStopHandle() stopHandle {
	return stopHandle{
		make(chan struct{}, 1),
		make(chan struct{}, 1),
	}
}

func (sh stopHandle) stop() {
	sh.requestStop <- struct{}{}
	<-sh.hasStopped
	close(sh.requestStop)
	close(sh.hasStopped)
}

func eventRegistrationsFromFunctionMappings(functionMappings []EventRegistration) []host.EventRegistration {
	eventRegistrations := make([]host.EventRegistration, 0, len(functionMappings))
	for _, mapping := range functionMappings {
		registration := host.EventRegistration{
			EventProviderID:      mapping.EventProviderID,
			EventProviderVersion: mapping.EventProviderVersion,
			SubscriberID:         mapping.SubscriberID,
			Config:               mapping.Config,
		}
		eventRegistrations = append(eventRegistrations, registration)
	}
	return eventRegistrations
}

//func uberHandlerFromFunctionMappings(mappings []EventRegistration) EventHandler {
//	subscriberIdToFunc := make(map[string]EventHandler, len(mappings))
//	for _, mapping := range mappings {
//		subscriberIdToFunc[mapping.SubscriberID] = mapping.Handler
//	}
//	return func(event host.Event) host.InvocationResult {
//		parts := strings.Split(event.Name, ":")
//		if len(parts) < 3 {
//			return host.InvocationResult{Data: nil, Status: host.Unhandled, TimeoutReason: ""}
//		}
//		subId := parts[2]
//		handler, ok := subscriberIdToFunc[subId]
//		if !ok {
//			return host.InvocationResult{Data: nil, Status: host.Unhandled, TimeoutReason: ""}
//		}
//		return handler(event)
//	}
//}

func errorFromEventRegistrationResponse(eventRegistrationsResponse *host.EventRegistrationsResponse) error {
	if !eventRegistrationsResponse.Success {
		message := "Error: " + eventRegistrationsResponse.FailureMessage + "\n"
		for _, result := range eventRegistrationsResponse.EventRegistrationResults {
			if !result.Success {
				message = message + result.FailureMessage + "\n"
			}
		}
		return errors.New(message)
	}
	return nil
}

func (worker *Worker) startHeartbeat(period time.Duration) stopHandle {
	sh := newStopHandle()
	client, log, workerID := worker.client, worker.config.Logger, worker.config.WorkerInstanceID
	go func() {
		defer func() {
			log.Println("Stopping heartbeat loop...")
			sh.hasStopped <- struct{}{}
		}()
	heartbeatLoop:
		for {
			log.Println("Sending heartbeat...")
			err := client.Heartbeat(host.HeartbeatRequest{WorkerInstanceID: workerID})
			if err != nil {
				log.Println("failed to send heartbeat: ", err)
			}
			select {
			case <-sh.requestStop:
				break heartbeatLoop
			case <-time.After(period):
				continue
			}
		}
	}()
	return sh
}

type Counter struct {
	Count     int
	Condition sync.Cond
}

func (counter *Counter) Inc() {
	counter.Condition.L.Lock()
	defer counter.Condition.L.Unlock()
	counter.Count++
	counter.Condition.Broadcast()
}

func (counter *Counter) Get() int {
	counter.Condition.L.Lock()
	defer counter.Condition.L.Unlock()
	return counter.Count
}

func (counter *Counter) Dec() {
	counter.Condition.L.Lock()
	defer counter.Condition.L.Unlock()
	counter.Count--
	counter.Condition.Broadcast()
}

type intPredicate = func(value int) bool

func (counter *Counter) WaitUntil(pred intPredicate) chan struct{} {
	result := make(chan struct{}, 1)
	go func() {
		counter.Condition.L.Lock()
		defer counter.Condition.L.Unlock()
		for !pred(counter.Count) {
			counter.Condition.Wait()
		}
		result <- struct{}{}
	}()
	return result
}

func makeSafeHandler(handler EventHandler) EventHandler {
	return func(event host.Event) (result host.InvocationResult, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panicked while invoking handler: %v", r)
			}
		}()
		result, err = handler(event)
		return
	}
}

func (worker *Worker) startHandleEvents(handler EventHandler) stopHandle {
	sh := newStopHandle()
	client, log, workerID, concurrency := worker.client, worker.config.Logger, worker.config.WorkerInstanceID, worker.config.Concurrency
	inProgress := Counter{Count: 0, Condition: sync.Cond{L: &sync.Mutex{}}}
	handler = makeSafeHandler(handler)
	go func() {
		defer func() {
			sh.hasStopped <- struct{}{}
		}()
	pollingLoop:
		for {

			select {
			case <-sh.requestStop:
				break pollingLoop
			default:
			}

			events, err := client.GetEvents(host.GetEventsRequest{
				WorkerInstanceID: workerID,
				Count:            concurrency - inProgress.Get(),
			})
			if err != nil {
				log.Println("failed to get events: ", err)
			}
			for _, event := range events {
				inProgress.Inc()
				go func(event host.Event) {
					defer inProgress.Dec()

					log.Printf("Processing event: %+v.\n", event)
					result, _err := handler(event)
					if _err != nil {
						log.Println("handler returned error: ", _err)
						result = host.InvocationResult{ Data: _err.Error(), Status: invocationStatus.Failed, TimeoutReason: ""}
					}
					_err = client.CompleteEvent(host.CompleteEventRequest{
						EventID: event.ID,
						Result:  result,
					})
					if _err != nil {
						log.Println("failed to complete the event:", _err)
					}
				}(event)
			}

			if len(events) <= 0 {
				log.Println("Got no events, taking nap.")
				select {
				case <-sh.requestStop:
					break pollingLoop
				case <-time.After(time.Millisecond * 500):
					continue
				}
			}

			select {
			case <-sh.requestStop:
				break pollingLoop
			case <-inProgress.WaitUntil(func(value int) bool { return value < concurrency }):
			}
		}
		log.Println("Stopping polling loop...")

		select {
		case <-inProgress.WaitUntil(func(value int) bool { return value == 0 }):
		case <-time.After(time.Second * 30):
			log.Println("couldn't stop polling loop in time; there are still some events being processed")
		}
	}()

	return sh
}
