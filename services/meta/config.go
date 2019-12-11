package meta

import (
	"errors"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/toml"
)

const (
	// DefaultLeaseDuration is the default duration for leases.
	DefaultLeaseDuration = 60 * time.Second

	// DefaultLoggingEnabled determines if log messages are printed for the meta service.
	DefaultLoggingEnabled = true

	DefaultDialTimeOut = 1 * time.Second
)

// Config represents the meta configuration.
type Config struct {
	Dir string `toml:"dir"`

	RetentionAutoCreate bool `toml:"retention-autocreate"`
	LoggingEnabled      bool `toml:"logging-enabled"`
}

// NewConfig builds a new configuration with default values.
func NewConfig() *Config {
	return &Config{
		RetentionAutoCreate: true,
		LoggingEnabled:      DefaultLoggingEnabled,
	}
}

// Validate returns an error if the config is invalid.
func (c *Config) Validate() error {
	if c.Dir == "" {
		return errors.New("Meta.Dir must be specified")
	}
	return nil
}

// Diagnostics returns a diagnostics representation of a subset of the Config.
func (c *Config) Diagnostics() (*diagnostics.Diagnostics, error) {
	return diagnostics.RowFromMap(map[string]interface{}{
		"dir": c.Dir,
	}), nil
}

type ProxyConfig struct {
	NodeId string `toml:"node-id"`

	BindAddress string        `toml:"bind-address"`
	RpcAddress  string        `toml:"rpc-address"`
	PeerIds     []string      `toml:"peer-ids"`
	DialTimeOut toml.Duration `toml:"timeout"`
	Endpoints   []string      `toml:"endpoints"`
}

func NewProxyConfig() *ProxyConfig {
	return &ProxyConfig{
		DialTimeOut: toml.Duration(DefaultDialTimeOut),
	}
}

func (c *ProxyConfig) Validate() error {
	if c.NodeId == "" || len(c.PeerIds) == 0 || c.BindAddress == "" || c.Endpoints == nil || c.RpcAddress == "" {
		return errors.New("error cluster config")
	}
	return nil
}

type RaftConfig struct {
	LocalID     string `toml:"node-id"`
	RaftAddress string `toml:"raft-address"`
	RaftDir     string `toml:"raft-dir"`
	//1=http://172.16.41.1:2379
	Servers     []string `toml:"servers"`
	Existing    bool
	JoinAddress string `toml:"join-address"`
	RpcAddress  string `toml:"rpc-address"`
}

func NewRaftConfig() *RaftConfig {
	return &RaftConfig{
		Existing: true,
	}
}

func (c *RaftConfig) Validate() error {
	if c.LocalID == "" || c.RaftAddress == "" || c.Servers == nil || c.RpcAddress == "" {
		return errors.New("error Raft config")
	}
	return nil
}
