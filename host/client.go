package host

type Client interface {
	Connect(req ConnectRequest) (ConnectResponse, error)
	Register(req EventRegistrationsRequest) (EventRegistrationsResponse, error)
	GetEvents(req GetEventsRequest) (GetEventsResponse, error)
	Heartbeat(req HeartbeatRequest) error
	CompleteEvent(req CompleteEventRequest) error
	Unregister(req UnregisterRequest) error
	Disconnect(req DisconnectRequest) error
}
