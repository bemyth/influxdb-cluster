package influxdb

type MetaNode struct {
	Name        string `toml:"name"`
	RaftAddress string `toml:"raft-bind-address"`
}

type DataNode struct {
	Name        string `toml:"name"`
	BindAddress string `toml:"bind-address"`
	RegAddress  string `toml:"register-address"`
}
