package tsdbserver

import "github.com/influxdata/influxdb/platform/tsdbserver/rpcservice"

type Config struct {
	Path string             `toml:"dir"`
	Rpc  *rpcservice.Config `toml:"rpc"`
}
