package metaservice

import (
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"net"
	"sync"
)

type MetaService struct {
	mu     sync.RWMutex
	dir    string
	store  *Store
	closed chan struct{}
}



func New(c *Config, name string, nodes map[string]influxdb.MetaNode, ln net.Listener) *MetaService {
	ms := &MetaService{
		dir: c.Dir,
	}
	ms.store = NewStore(name, nodes, ms.dir, ln)
	return ms
}
func (ms *MetaService) Open() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.store.Open(); err != nil {
		return err
	}
	return nil
}
func (ms *MetaService) MapShard(points models.Points) (map[uint64]models.Points, error) {
	ms.store.mapShards()
	return nil, nil
}
func (ms *MetaService)GetAddrByDnID(dnID uint64) (address string,err error){

}
func (ms *MetaService) GetDnsIDBySID(shardID uint64) (dnsID []uint64, err error) {
	ms.store.get
	return nil, nil
}