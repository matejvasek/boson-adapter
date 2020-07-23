package messageName

const (
	messageNamesPrefix           = "projectbrasilia.core."
	responseSuffix               = "response"
	Connect                      = messageNamesPrefix + "connect"
	ConnectResponse              = Connect + responseSuffix
	WorkerRegistration           = messageNamesPrefix + "workerregistration"
	WorkerRegistrationResponse   = WorkerRegistration + responseSuffix
	WorkerMessagePolling         = messageNamesPrefix + "messagepolling"
	WorkerMessagePollingResponse = WorkerMessagePolling + responseSuffix
	CompleteEvent                = messageNamesPrefix + "eventresult"
	CompleteEventResponse        = CompleteEvent + responseSuffix
	GetEvents                    = messageNamesPrefix + "getevents"
	GetEventsResponse            = GetEvents + responseSuffix
	Heartbeat                    = messageNamesPrefix + "heartbeat"
	HeartbeatResponse            = Heartbeat + responseSuffix
	ErrorResponse                = messageNamesPrefix + "errorresponse"
	Unregister                   = messageNamesPrefix + "unregister"
	UnregisterResponse           = Unregister + responseSuffix
	Disconnect                   = messageNamesPrefix + "disconnect"
	DisconnectResponse           = Disconnect + responseSuffix
)