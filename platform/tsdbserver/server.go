package tsdbserver

import (
	"github.com/influxdata/influxdb/platform/tsdbserver/rpcservice"
	"github.com/influxdata/influxdb/tsdb"
	"net"
)

type Server struct {
	name     string
	dir      string
	storeSvc *tsdb.Store
	rpcSvc   *rpcservice.RpcService

	ln net.Listener
}

func New(c *Config) *Server {
	s := &Server{}
	s.storeSvc = tsdb.NewStore(c.Path)
	s.rpcSvc = rpcservice.New(c.Rpc)
	s.rpcSvc.TSDBStore = s.storeSvc
	return s
}
func (s *Server) Open() error {
	s.storeSvc.Open()
	s.rpcSvc.Open()
	return nil
}

//TODO close services
func (s *Server) Close() error {
	return nil
}
