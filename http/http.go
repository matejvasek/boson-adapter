package http

import (
	"boson-adapter/host"
	"boson-adapter/host/messageName"
	"context"
	"encoding/json"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/cloudevents/sdk-go/v2/event"
)

type sendFn func(req host.Message, resp host.Message) error

func NewHTTPHostClient(url string) (host.Client, error) {
	protocol, err := cloudevents.NewHTTP(cloudevents.WithTarget(url))
	if err != nil {
		return nil, err
	}
	cloudEventClient, err := cloudevents.NewClient(protocol, client.WithForceStructured(), client.WithUUIDs(), client.WithTimeNow())
	if err != nil {
		return nil, err
	}
	send := func(in host.Message, out host.Message) error {
		evtToSend, err := messageToEvent(in)
		if err != nil {
			return err
		}
		receivedEvt, resp := cloudEventClient.Request(context.Background(), *evtToSend)
		if cloudevents.IsUndelivered(resp) || cloudevents.IsNACK(resp) {
			if receivedEvt != nil && receivedEvt.Type() == messageName.ErrorResponse {
				s := ""
				err = receivedEvt.DataAs(&s)
				if err != nil {
					return err
				}
				errResp := host.ErrorResponse{}
				err = json.Unmarshal([]byte(s), &errResp)
				if err != nil {
					return err
				}
				return &errResp
			}
			return resp
		}
		err = eventToMessage(receivedEvt, out)
		if err != nil {
			return err
		}
		return nil
	}
	return sendFn(send), nil
}

func (send sendFn) Connect(req host.ConnectRequest) (resp host.ConnectResponse, err error) {
	err = send(&req, &resp)
	return
}

func (send sendFn) Register(req host.EventRegistrationsRequest) (resp host.EventRegistrationsResponse, err error) {
	err = send(&req, &resp)
	return
}

func (send sendFn) GetEvents(req host.GetEventsRequest) (resp host.GetEventsResponse, err error) {
	err = send(&req, &resp)
	return
}

func (send sendFn) Heartbeat(req host.HeartbeatRequest) (err error) {
	err = send(&req, nil)
	return
}

func (send sendFn) CompleteEvent(req host.CompleteEventRequest) (err error) {
	err = send(&req, nil)
	return
}

func (send sendFn) Unregister(req host.UnregisterRequest) (err error) {
	err = send(&req, nil)
	return
}

func (send sendFn) Disconnect(req host.DisconnectRequest) (err error) {
	err = send(&req, nil)
	return
}

func eventToMessage(evt *event.Event, out host.Message) (err error) {
	if evt == nil {
		return nil
	}

	if evt.Type() == messageName.ErrorResponse {
		s := ""
		err = evt.DataAs(&s)
		if err != nil {
			return err
		}
		errResp := host.ErrorResponse{}
		err = json.Unmarshal([]byte(s), &errResp)
		if err != nil {
			return err
		}
		return fmt.Errorf("got error from ProjectB Host: %+v", errResp)
	}
	if evt.Type() != out.Name() {
		return fmt.Errorf("received message of unexpected type")
	}

	s := ""
	err = evt.DataAs(&s)
	if err != nil {
		return err
	}
	err = json.Unmarshal([]byte(s), out)
	if err != nil {
		return err
	}

	return nil
}

func messageToEvent(message host.Message) (*event.Event, error) {
	ceType := message.Name()
	evt := cloudevents.NewEvent()
	evt.SetType(ceType)
	evt.SetSource("/ofKnativePollingAdapter")

	b, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	err = evt.SetData(cloudevents.ApplicationJSON, string(b))
	if err != nil {
		return nil, err
	}
	return &evt, nil
}
