package raft_store

import (
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/influxdata/influxdb"
	"net"
	"os"
	"path/filepath"
	"time"
)

type raftState struct {
	LocalID   raft.ServerID
	LocalAddr raft.ServerAddress
	LocalPath string

	raft      *raft.Raft
	ln        net.Listener
	transport *raft.NetworkTransport
	rstore    *raftboltdb.BoltStore

	// TODO raft启动完成后需要校验该信息
	servers []raft.Server
}

func NewRaftState(localID string, nodes map[string]influxdb.MetaNode, path string, ln net.Listener) *raftState {
	//lnode := nodes[localID]
	rs := &raftState{
		LocalID:   raft.ServerID(localID),
		LocalAddr: raft.ServerAddress(nodes[localID].RaftAddress),
		LocalPath: path,
		ln:        ln,
	}
	for _, v := range nodes {
		rs.servers = append(rs.servers, raft.Server{
			ID:      raft.ServerID(v.Name),
			Address: raft.ServerAddress(v.RaftAddress),
		})
	}
	return rs
}

func (rs *raftState) open(fsm raft.FSM) error {
	config := raft.DefaultConfig()
	//config.LogOutput = ioutil.Discard
	config.LocalID = rs.LocalID

	raftlayer := newRaftLayer(rs.LocalAddr, rs.ln)

	rs.transport = raft.NewNetworkTransport(raftlayer, 3, 10*time.Second, config.LogOutput)

	store, err := raftboltdb.NewBoltStore(filepath.Join(rs.LocalPath, metaFile))
	if err != nil {
		return fmt.Errorf("new bolt store : %s", err)
	}
	rs.rstore = store

	snapshots, err := raft.NewFileSnapshotStore(rs.LocalPath, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	ra, err := raft.NewRaft(config, fsm, store, store, snapshots, rs.transport)
	if err != nil {
		return fmt.Errorf("new raft : %s", err)
	}
	rs.raft = ra
	if future := rs.raft.BootstrapCluster(raft.Configuration{
		Servers: rs.servers,
	}); future != nil {
		return err
	}
	return nil
}
