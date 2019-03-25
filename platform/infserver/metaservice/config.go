package metaservice

import "errors"

const (
	DefaultRetentionAutoCreate = true
	DefaultLoggingEnable       = true
)

type Config struct {
	Dir                 string `toml:"dir"`
	RetentionAutoCreate bool   `toml:"retention-autocreate"`
	LoggingEnabled      bool   `toml:"logging-enabled"`
}

func NewConfig() *Config {
	return &Config{
		RetentionAutoCreate: DefaultRetentionAutoCreate,
		LoggingEnabled:      DefaultLoggingEnable,
	}
}
func (c *Config) Validate() error {
	if c.Dir == "" {
		return errors.New("Meta.Dir must be specified")
	}
	return nil
}
