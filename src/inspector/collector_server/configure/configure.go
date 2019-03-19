package conf

const (
	SendFailDirectory = "send_fail"
)

type Configuration struct {
	ConfigServerAddress  string // config server address: ip:port
	ConfigServerUsername string // config server username
	ConfigServerPassword string // config server password
	ConfigServerDB       string // config server db
	ConfigServerInterval int    // config server watch interval
	HeartbeatInterval    int    // heartbeat interval
	CollectorServerPort  int    // collector server port
	MonitorPort          int    // http monitor port
	SystemProfile        int    // profiling port
	WorkPath             string // work path

	// below variables are generated
	CollectorServerAddress string // collector server address: ip:port
	WorkPathSendFail       string // send fail path
}

var Options Configuration
