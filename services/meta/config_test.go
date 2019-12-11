package meta_test

import (
	"testing"

	"context"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/coreos/etcd/clientv3"
	"github.com/influxdata/influxdb/services/meta"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c meta.Config
	if _, err := toml.Decode(`
dir = "/tmp/foo"
logging-enabled = false
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Dir != "/tmp/foo" {
		t.Fatalf("unexpected dir: %s", c.Dir)
	} else if c.LoggingEnabled {
		t.Fatalf("unexpected logging enabled: %v", c.LoggingEnabled)
	}
}
func TestGet(t *testing.T) {
	fmt.Println("starting")
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"172.16.41.15:2379"},
		DialTimeout: meta.DefaultDialTimeOut,
	})
	if err != nil {

	}
	kv := clientv3.NewKV(client)
	getResp, err := kv.Get(context.TODO(), "/test/a")
	if err != nil {
		//log.Info(err)
	}
	fmt.Println(getResp.Header)
	fmt.Println(getResp.Count)
	fmt.Println(getResp.Kvs)
	fmt.Println(getResp.More)

}
