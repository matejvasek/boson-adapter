package host

import (
	"boson-adapter/host/invocationStatus"
	"boson-adapter/host/messageName"
	"fmt"
	"strings"
)

type Message interface {
	Name() string
}

type ConnectRequest struct {
	ApplicationID      string
	ApplicationVersion string
	WorkerInstanceID   string
}

type ConnectResponse struct {
	ChannelConfigurations          map[string]map[string]string
	WorkerHearbeatTimeoutInSeconds int
}

type HeartbeatRequest struct {
	WorkerInstanceID string
}

type HeartbeatResponse struct{}

type EventRegistration struct {
	EventProviderID      string
	EventProviderVersion string
	SubscriberID         string
	Config               map[string]string
}

type EventRegistrationsRequest struct {
	WorkerInstanceID   string
	EventRegistrations []EventRegistration
}

type EventRegistrationResult struct {
	EventProviderID      string
	EventProviderVersion string
	SubscriberID         string
	Success              bool
	FailureMessage       string
}

type EventRegistrationsResponse struct {
	EventRegistrationResults []EventRegistrationResult
	Success                  bool
	FailureMessage           string
}

type GetEventsRequest struct {
	Count            int
	WorkerInstanceID string
}

type Event struct {
	ID   string
	Name string
}

type GetEventsResponse []Event

type CompleteEventRequest struct {
	EventID string
	Result  InvocationResult
}

type InvocationResult struct {
	Data          interface{}
	Status        invocationStatus.Type
	TimeoutReason string
}

// probably unused, as there is probably no response body
type CompleteEventResponse struct{}

type UnregisterRequest struct {
	WorkerInstanceID string
}

// probably unused, as there is probably no response body
type UnregisterResponse struct{}

type DisconnectRequest struct {
	WorkerInstanceID string
}

// probably unused, as there is probably no response body
type DisconnectResponse struct{}

type ErrorResponse struct {
	ErrorCode    string
	ErrorDetails string
}

func (msg *ErrorResponse) Error() string {
	errCode, errDetail := msg.ErrorCode, msg.ErrorDetails
	parts := make([]string, 0, 2)
	if errCode != "" {
		parts = append(parts, fmt.Sprintf("error code: '%s'", errCode))
	}
	if errDetail != "" {
		parts = append(parts, fmt.Sprintf("error message: '%s'", errDetail))
	}
	return strings.Join(parts, ",")
}

func (msg *ConnectResponse) Name() string {
	return messageName.ConnectResponse
}

func (msg *DisconnectRequest) Name() string {
	return messageName.Disconnect
}

func (msg *EventRegistrationsResponse) Name() string {
	return messageName.WorkerRegistrationResponse
}

func (msg *HeartbeatRequest) Name() string {
	return messageName.Heartbeat
}

func (msg *GetEventsRequest) Name() string {
	return messageName.GetEvents
}

func (msg *UnregisterRequest) Name() string {
	return messageName.Unregister
}

func (msg *HeartbeatResponse) Name() string {
	return messageName.HeartbeatResponse
}

func (msg *CompleteEventRequest) Name() string {
	return messageName.CompleteEvent
}

func (msg *GetEventsResponse) Name() string {
	return messageName.GetEventsResponse
}

func (msg *EventRegistrationsRequest) Name() string {
	return messageName.WorkerRegistration
}

func (msg *ConnectRequest) Name() string {
	return messageName.Connect
}

func (msg *CompleteEventResponse) Name() string {
	return messageName.CompleteEventResponse
}

func (msg *UnregisterResponse) Name() string {
	return messageName.UnregisterResponse
}

func (msg *DisconnectResponse) Name() string {
	return messageName.DisconnectResponse
}

func (msg *ErrorResponse) Name() string {
	return messageName.ErrorResponse
}