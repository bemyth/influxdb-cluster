package raft_store

import (
	"github.com/hashicorp/raft"
	"net"
	"time"
)

const (
	MuxHeader = 8
)

type raftLayer struct {
	addr   *raftLayerAddr
	ln     net.Listener
	conn   chan net.Conn
	closed chan struct{}
}

type raftLayerAddr struct {
	addr raft.ServerAddress
}

func (r *raftLayerAddr) Network() string {
	return "tcp"
}
func (r *raftLayerAddr) String() string {
	return string(r.addr)
}
func newRaftLayer(addr raft.ServerAddress, ln net.Listener) *raftLayer {
	return &raftLayer{
		addr:   &raftLayerAddr{addr: addr},
		ln:     ln,
		conn:   make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

func (l *raftLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", string(addr), timeout)
	if err != nil {
		return nil, err
	}
	_, err = conn.Write([]byte{MuxHeader})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, err
}
func (l *raftLayer) Accept() (net.Conn, error) {
	return l.ln.Accept()
}
func (l *raftLayer) Addr() net.Addr {
	return l.addr
}
func (l *raftLayer) Close() error {
	return l.ln.Close()
}
