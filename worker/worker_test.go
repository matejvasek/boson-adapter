package worker

import (
	"boson-adapter/host"
	"boson-adapter/host/invocationStatus"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

type MockHostClient struct {
	cond                         sync.Cond
	connectInvocationCount       int
	registerInvocationCount      int
	getEventsInvocationCount     int
	heartBeatInvocationCount     int
	completeEventInvocationCount int
	unregisterInvocationCount    int
	disconnectInvocationCount    int
	ConnectResponse              host.ConnectResponse
	EventRegistrationResponse    host.EventRegistrationsResponse
	EventRegistrationRequest     host.EventRegistrationsRequest
	Events                       []host.Event
	CompleteEventRequests        []host.CompleteEventRequest
}

func (m *MockHostClient) Connect(req host.ConnectRequest) (host.ConnectResponse, error) {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	defer m.cond.Broadcast()
	defer func() { m.connectInvocationCount++ }()

	return m.ConnectResponse, nil
}

func (m *MockHostClient) Register(req host.EventRegistrationsRequest) (host.EventRegistrationsResponse, error) {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	defer m.cond.Broadcast()
	defer func() { m.registerInvocationCount++ }()

	m.EventRegistrationRequest = req

	return m.EventRegistrationResponse, nil
}

func (m *MockHostClient) GetEvents(req host.GetEventsRequest) (host.GetEventsResponse, error) {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	defer m.cond.Broadcast()
	defer func() { m.getEventsInvocationCount++ }()

	count := req.Count
	if count > len(m.Events) {
		count = len(m.Events)
	}
	x := len(m.Events) - count

	newEvents := m.Events[:x]
	result := m.Events[x:]
	m.Events = newEvents

	return result, nil
}

func (m *MockHostClient) Heartbeat(req host.HeartbeatRequest) error {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	defer m.cond.Broadcast()
	defer func() {
		m.heartBeatInvocationCount++
	}()

	return nil
}

func (m *MockHostClient) CompleteEvent(req host.CompleteEventRequest) error {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	defer m.cond.Broadcast()
	defer func() { m.completeEventInvocationCount++ }()
	m.CompleteEventRequests = append(m.CompleteEventRequests, req)

	return nil
}

func (m *MockHostClient) Unregister(req host.UnregisterRequest) error {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	defer m.cond.Broadcast()
	defer func() { m.unregisterInvocationCount++ }()

	return nil
}

func (m *MockHostClient) Disconnect(req host.DisconnectRequest) error {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	defer m.cond.Broadcast()
	defer func() { m.disconnectInvocationCount++ }()

	return nil
}

func (m *MockHostClient) WaitUntil(predicate func(m *MockHostClient) bool) chan struct{} {
	result := make(chan struct{}, 1)
	go func() {
		m.cond.L.Lock()
		defer m.cond.L.Unlock()
		for !predicate(m) {
			m.cond.Wait()
		}
		result <- struct{}{}
	}()
	return result
}

type T struct {
}

func (t *T) Write(p []byte) (n int, err error) {
	return len(p), err
}

func TestWorker_Start(t *testing.T) {

	type fields struct {
		config Config
		client host.Client
	}

	type args struct {
		stopChan chan struct{}
	}

	events := make([]host.Event, 0, 1<<10)
	for i := cap(events)-1; i >= 0; i-- {
		events = append(events, host.Event{ID: strconv.Itoa(i), Name: "app:1.0:foo"})
	}

	mockHostClient := MockHostClient{
		cond: sync.Cond{L: &sync.Mutex{}},
		ConnectResponse: host.ConnectResponse{
			ChannelConfigurations:          map[string]map[string]string{"http": {"url": "http://localhost:5001"}},
			WorkerHearbeatTimeoutInSeconds: 1},
		EventRegistrationResponse: host.EventRegistrationsResponse{
			EventRegistrationResults: []host.EventRegistrationResult{},
			Success:                  true,
			FailureMessage:           "",
		},
		Events:                events,
		CompleteEventRequests: make([]host.CompleteEventRequest, 0, len(events)),
	}

	workerConfig := Config{
		ApplicationID:      "app",
		ApplicationVersion: "1.0",
		EventRegistrations: []EventRegistration{
			{
				"Timer",
				"0.1",
				"foo",
				map[string]string{"schedule": "*/1 * * * * *"},
			},
		},
		Logger: log.New(&T{}, "worker test: ", log.Lmicroseconds),
		Handler: func(event host.Event) (result host.InvocationResult, err error) {
			i, err := strconv.Atoi(event.ID)
			if err == nil && i == 0 {
				ip := (*int)(nil)
				*ip = 0
			}
			return host.InvocationResult{ Data: event.ID, Status: invocationStatus.Succeeded }, nil
		},
		Concurrency: 128,
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Start-Stop test",
			fields{
				workerConfig,
				&mockHostClient,
			},
			args{make(chan struct{}, 1)},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker := Worker{
				config: tt.fields.config,
				client: tt.fields.client,
			}
			stopChan := tt.args.stopChan
			mockHostClient := tt.fields.client.(*MockHostClient)

			workerHasStopped := make(chan struct{}, 1)
			var err error
			go func() {
				err = worker.Start(tt.args.stopChan)
				workerHasStopped <- struct{}{}
			}()

			select {
			case <-time.After(time.Second * 1):
				t.Error("The worker hasn't registered in time.")
			case <-mockHostClient.WaitUntil(func(m *MockHostClient) bool { return m.registerInvocationCount >= 1 }):
			}

			if !(mockHostClient.EventRegistrationRequest.EventRegistrations[0].SubscriberID == tt.fields.config.EventRegistrations[0].SubscriberID) {
				t.Error("Bad registration, subscriberID mismatch.")
			}

			select {
			case <-time.After(time.Second * 3):
				t.Error("The worker hasn't sent heartbeat in time.")
			case <-mockHostClient.WaitUntil(func(m *MockHostClient) bool { return m.heartBeatInvocationCount >= 1 }):
			}

			select {
			case <-time.After(time.Second * 30):
				t.Error("The worker hasn't precessed events in time.")
			case <-mockHostClient.WaitUntil(func(m *MockHostClient) bool { return m.completeEventInvocationCount >= len(events) }):
			}

			stopChan <- struct{}{}
			select {
			case <-time.After(time.Second * 5):
				t.Error("The worker hasn't stopped in time.")
			case <-workerHasStopped:
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}

			if mockHostClient.unregisterInvocationCount < 1 {
				t.Error("The Unregister function hasn't been called.")
			}
			if mockHostClient.disconnectInvocationCount < 1 {
				t.Error("The Disconnect function hasn't been called.")
			}

		})
	}
}
