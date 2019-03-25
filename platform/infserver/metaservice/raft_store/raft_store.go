package raft_store

import (
	"github.com/hashicorp/raft"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models/meta"
	"io"
	"net"
	"sync"
)

const (
	metaFile = "meta.db"
)

type RaftStore struct {
	mu        sync.RWMutex
	data      *meta.Data
	raftState *raftState
}

func NewRaftStore(localID string, nodes map[string]influxdb.MetaNode, path string, ln net.Listener) *RaftStore {
	return &RaftStore{
		raftState: NewRaftState(localID, nodes, path, ln),
	}
}
func (rs *RaftStore) Open() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	return rs.raftState.open(rs)
}
func (rs *RaftStore) Apply(log *raft.Log) interface{} {
	return nil
}

func (rs *RaftStore) Snapshot() (raft.FSMSnapshot, error) {
	return &storeFFSMSnapshot{Data: rs.data}, nil
}
func (rs *RaftStore) Restore(ir io.ReadCloser) error {
	return nil
}

type storeFFSMSnapshot struct {
	Data *meta.Data
}

func (s *storeFFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		p, err := s.Data.MarshBinary()
		if err != nil {
			return err
		}
		if _, err := sink.Write(p); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		sink.Cancel()
		return err
	}
	return nil

}
func (s *storeFFSMSnapshot) Release() {}
