package boson

import (
	"boson-adapter/host"
	"boson-adapter/host/invocationStatus"
	"boson-adapter/worker"
	"context"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/google/uuid"
	"log"
	"strings"
)

type ProjectBEvent struct {
	EventProviderID      string
	EventProviderVersion string
	Config               map[string]string
}

type KnativeEvent struct {
	Source string
	Type   string
}

type ProjectBKnativeEventMapping struct {
	ProjectBEvent ProjectBEvent
	KnativeEvent  KnativeEvent
}

type AdapterConfig struct {
	ApplicationID      string
	ApplicationVersion string
	ProjectBHostURL    string
	BosonFunctionURL   string
	Concurrency        int
	Mapping            []ProjectBKnativeEventMapping
	Logger             *log.Logger
}

type Adapter struct {
	config AdapterConfig
}

func NewAdapter(config AdapterConfig) (*Adapter, error) {
	return &Adapter{config: config}, nil
}

func (adapter *Adapter) Start(stopChan chan struct{}) error {

	log := adapter.config.Logger

	protocol, err := cloudevents.NewHTTP(cloudevents.WithTarget(adapter.config.BosonFunctionURL))
	if err != nil {
		return err
	}
	cloudEventClient, err := cloudevents.NewClient(
		protocol,
		client.WithTimeNow(),
	)
	if err != nil {
		return err
	}

	mappings := adapter.config.Mapping
	subsIDtoCE := make(map[string]struct{ ceType, ceSource string }, len(mappings))
	eventRegistrations := make([]worker.EventRegistration, 0, len(mappings))
	for _, mapping := range mappings {
		subUUID, err := uuid.NewRandom()
		if err != nil {
			return err
		}
		subsID := subUUID.String()
		eventRegistrations = append(eventRegistrations, worker.EventRegistration{
			SubscriberID:         subsID,
			EventProviderID:      mapping.ProjectBEvent.EventProviderID,
			EventProviderVersion: mapping.ProjectBEvent.EventProviderVersion,
			Config:               mapping.ProjectBEvent.Config,
		})
		subsIDtoCE[subsID] = struct{ ceType, ceSource string }{
			ceType:   mapping.KnativeEvent.Type,
			ceSource: mapping.KnativeEvent.Source,
		}
	}
	ctx, cancelCloudEventsClient := context.WithCancel(context.Background())
	defer cancelCloudEventsClient()
	eventHandler := func(event host.Event) (host.InvocationResult, error) {

		parts := strings.Split(event.Name, ":")
		if len(parts) < 3 {
			return host.InvocationResult{}, fmt.Errorf("invalide value of `Name` in event: `%s`", event.Name)
		}
		subsID := parts[2]

		ceProps, ok := subsIDtoCE[subsID]
		if !ok {
			log.Printf("got unexpected subscriber ID: %+v\n", event)
			return host.InvocationResult{}, fmt.Errorf("unexpected subscriber ID: `%s`", subsID)
		}

		eventToSend := cloudevents.NewEvent()
		eventToSend.SetID(event.ID)
		eventToSend.SetType(ceProps.ceType)
		eventToSend.SetSource(ceProps.ceSource)
		_err := eventToSend.SetData(cloudevents.ApplicationJSON, nil) // there is no data in ProjectB event
		if _err != nil {
			return host.InvocationResult{}, _err
		}
		receivedEvent, protocolResult := cloudEventClient.Request(ctx, eventToSend)
		if !cloudevents.IsACK(protocolResult) {
			log.Println("failed to send event to boson function: ", protocolResult.Error())
			return host.InvocationResult{}, fmt.Errorf("failed to call boson function: `%s`", protocolResult.Error())
		}
		return host.InvocationResult{Data: receivedEvent.Data(), Status: invocationStatus.Succeeded, TimeoutReason: ""}, nil
	}
	workerUUID, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	projectbWorkerConfig := worker.Config{
		ApplicationID:      adapter.config.ApplicationID,
		ApplicationVersion: adapter.config.ApplicationVersion,
		WorkerInstanceID:   workerUUID.String(),
		Concurrency:        adapter.config.Concurrency,
		HostURL:            adapter.config.ProjectBHostURL,
		Handler:            eventHandler,
		EventRegistrations: eventRegistrations,
		Logger:             adapter.config.Logger,
	}
	projectbWorker, err := worker.NewWorker(projectbWorkerConfig)
	if err != nil {
		return err
	}

	projectbWorkerStopChan := make(chan struct{}, 1)
	projectbWorkerHasStoppedChan := make(chan struct{}, 1)
	go func() {
		defer func() {
			projectbWorkerHasStoppedChan <- struct{}{}
		}()
		_err := projectbWorker.Start(projectbWorkerStopChan)
		if _err != nil {
			log.Println("error while starting projectb worker: ", _err)
			stopChan <- struct{}{}
		}
	}()

	<-stopChan
	close(stopChan)
	// aborts ongoing http requests
	cancelCloudEventsClient()
	// stop projectb worker and wait for it to end
	projectbWorkerStopChan <- struct{}{}
	<-projectbWorkerHasStoppedChan
	return nil
}
