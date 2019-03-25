package metaservice

import (
	"fmt"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/models/meta"
	"github.com/influxdata/influxdb/platform/infserver/metaservice/raft_store"
	"net"
	"os"
	"sync"
)

type Store struct {
	mu        sync.RWMutex
	cacheData *meta.Data

	dir       string
	raftStore *raft_store.RaftStore
	changed   chan struct{}

	closed chan struct{}
}

func NewStore(localID string, nodes map[string]influxdb.MetaNode, path string, ln net.Listener) *Store {
	s := &Store{
		dir:     path,
		changed: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	s.raftStore = raft_store.NewRaftStore(localID, nodes, path, ln)
	return s
}

func (s *Store) Open() error {
	if err := os.MkdirAll(s.dir, 0777); err != nil {
		return fmt.Errorf("mkdir all: %s", err)
	}
	return s.raftStore.Open()
}

func (s *Store)retentionPolicy() {
	return
}
func (s *Store)mapShards(database,retentionPolicy string, points []models.Point){

}
func (s *Store) GetDnsIDBySID(shardID uint64) (dnsID []uint64, err error) {
	s.mu.RUnlock()
	defer s.mu.RUnlock()

	return nil, nil
}
