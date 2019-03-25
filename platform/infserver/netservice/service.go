package netservice

import (
	"net"
	"net/http"
	"sync"
)

type Service struct {
	mu      sync.RWMutex
	ln      net.Listener
	handler *Handler
	addr    string
}

func New(c *Config) *Service {
	s := &Service{
		addr: c.BindAddress,
	}
	s.handler = &Handler{}
	return s
}

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln

	go func() {
		s.serve()
	}()
	return nil
}

func (s *Service) serve() error {
	return http.Serve(s.ln, s.handler)
}
