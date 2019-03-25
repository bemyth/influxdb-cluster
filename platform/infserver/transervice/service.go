package transervice

import (
	"context"
	"github.com/influxdata/influxdb/models"
	pb "github.com/influxdata/influxdb/rpc/infrpc"
	"google.golang.org/grpc"
)


type TranService struct {
	ctx context.Context
}

func New() *TranService {
	return &TranService{}
}

func (ts *TranService) Open() error {
	return nil
}

func (ts *TranService) WriteToNodeAddress(target string, sID uint64, pts models.Points) error {
	conn,err := grpc.Dial(target)
	if err != nil{
		return err
	}
	bpts := make([][]byte,len(pts))
	for i,pt := range pts{
		bpt,err := pt.MarshalBinary()
		if err != nil{
			return err
		}
		bpts[i] = bpt
	}
	req := pb.WritePointsRequest{ShardID:sID,Points:bpts}
	_,err = pb.NewInfRPCClient(conn).WritePoints(ts.ctx,&req)
	if err != nil{
		return err
	}
	return nil
}
