package model

const (
	instanceIp   = "ip"
	instancePort = "port"

	// instance field
	HostName     = "host"
	PidName      = "pid"
	HidName      = "hid"
	UsernameName = "username"
	PasswordName = "password"
	DBTypeName   = "dbType" // mysql, redis, mongodb
	Count        = "count"
	Interval     = "interval"
	Commands     = "cmds"
)

type Instance struct {
	// instance info
	Pid  uint32 // parent id
	Hid  int32  // instance id
	Addr string // ip:port

	// server info
	Username string
	Password string
	DBType   string // mysql, redis, mongodb

	/* above information are stored in the taskList and taskDistribute collection*/

	// "count" and "interval" fields are got from meta collection
	Count    int
	Interval int

	Commands []string
}
