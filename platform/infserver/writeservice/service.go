package writeservice

import (
	"github.com/influxdata/influxdb/models"
)

type MetaService interface {
	MapShard(database,retentionPolicy string, points models.Points) (map[uint64]models.Points, error) //map[shardID]Points
	GetDnsIDBySID(shardID uint64) (dnsID []uint64, err error)
	GetAddrByDnID(dnID uint64) (address string,err error)
}

type Transmitter interface {
	WriteToNodeAddress(target string, sID uint64, pts models.Points) error
}
type WriteService struct {
	MetaService MetaService
	Transmitter Transmitter
}

func New() *WriteService {
	ws := &WriteService{}
	//ws.MetaService = ms
	//ws.Transmitter = tram
	return ws
}

func (ws *WriteService) Open() error {
	return nil
}

func (ws *WriteService) writeToShard(sid uint64, pts models.Points) error {
	dnsID, err := ws.MetaService.GetDnsIDBySID(sid)
	if err != nil {
		return err
	}
	for _, dn := range dnsID {
		//TODO 高可用考虑是否重写，以及是否可以重写，若不可重写，如何保证一致性
		addr,err := ws.MetaService.GetAddrByDnID(dn)
		if err != nil{
			return err
		}
		if err := ws.Transmitter.WriteToNodeAddress(addr, sid, pts); err != nil {
			return err
		}
	}
	return err
}

func (ws *WriteService) Write(database, retentionPolicy string, pts models.Points) error {
	spts, err := ws.MetaService.MapShard(database,retentionPolicy,pts)
	for sid, pts := range spts {
		ws.writeToShard(sid, pts)
	}
	return err
}
