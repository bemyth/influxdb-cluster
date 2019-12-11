package meta

//import (
//	"errors"
//	"fmt"
//	"github.com/coreos/etcd/clientv3"
//	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
//	"github.com/gogo/protobuf/proto"
//	internal "github.com/influxdata/influxdb/services/meta/internal"
//	"strconv"
//	"time"
//)

type Agent interface {
	Register(NodeId uint64, Host string, dir string)
	PutCacheData(dir string, data *Data) error
	GetCacheData(dir string) (*Data, error)
	InitNodeState(dir string) error
}

//func (c *Client) InitNodeState(dir string) error {
//	kv := clientv3.NewKV(c.c)
//	resp, err := kv.Get(c.ctx, dir, clientv3.WithPrefix())
//	if err != nil {
//		return err
//	}
//	for _, kv := range resp.Kvs {
//		uid, err := AgentDirToNodeID(kv.Key)
//		if err != nil {
//			c.logger.Info("unknow message format in etcd")
//			//fmt.Println(fmt.Sprintf("unknow message type"))
//			return err
//		}
//		c.NodeState[uid] = Online
//		c.logger.Info(fmt.Sprintf("node %d has online", uid))
//	}
//	return nil
//}

//
////Register register value to dir --- such as /inflxudb/agent/:nodeid localhost:8086
//func (c *Client) Register(NodeId uint64, Host string, dir string) {
//	sid := strconv.Itoa(int(NodeId))
//	key := dir + "/" + sid
//	var lease *clientv3.LeaseGrantResponse = nil
//	for {
//		if lease == nil {
//			leaseResp, err := c.c.Grant(c.ctx, 10)
//			if err != nil {
//				c.logger.Error("Proxy Registrate Failed On leasing:" + err.Error())
//			} else {
//				lease = leaseResp
//				_, err = c.c.Put(c.ctx, key, Host, clientv3.WithLease(lease.ID))
//				if err != nil {
//					c.logger.Error("Registrate ServerState Failed:" + err.Error())
//				} else {
//					c.logger.Debug("Registrate Server Success with :" + key + ":" + Host)
//				}
//			}
//		} else {
//			_, err := c.c.KeepAlive(c.ctx, lease.ID)
//			if err != nil {
//				c.logger.Error("Keep Alive Failed:" + err.Error())
//				if err == rpctypes.ErrLeaseNotFound {
//					lease = nil
//				}
//				continue
//			}
//		}
//		time.Sleep(time.Duration(1) * time.Second)
//	}
//}
//
//func (c *Client) PutCacheData(dir string, data *Data) error {
//	//上当前版本版本置为上一个版本
//	data.PreVersion = c.version
//	value, err := DataToString(data)
//	if err != nil {
//		return err
//	}
//
//	kv := clientv3.NewKV(c.c)
//
//	_, err = kv.Put(c.ctx, dir, value)
//
//	if err != nil {
//		return err
//	}
//	return nil
//
//}

//func (c *Client) GetCacheData(dir string) (*Data, error) {
//	kv := clientv3.NewKV(c.c)
//	getRsp, err := kv.Get(c.ctx, dir)
//	if err != nil {
//		return nil, err
//	}
//	//1.远端没有数据，没有任何节点上传新数据,当前版本置为head版本
//	//2.远端有数据但没获取到？等待处理
//	if len(getRsp.Kvs) == 0 {
//		d := &Data{}
//		c.version = getRsp.Header.GetRevision()
//		return d, nil
//	}
//	//1.远端有数据，集群非第一次启动。同步远端数据
//	if len(getRsp.Kvs) == 1 {
//		data := &internal.Data{}
//		if err := proto.Unmarshal(getRsp.Kvs[0].Value, data); err != nil {
//			c.logger.Error("Read error CacheData format in GetCacheData")
//			return nil, err
//		}
//		d := c.cacheData.Clone()
//		d.unmarshal(data)
//		c.version = getRsp.Header.GetRevision()
//		return d, nil
//	}
//	return nil, errors.New("Unknow error in GetCacheData")
//}
//
//func DataToString(data *Data) (string, error) {
//	bytes, err := proto.Marshal(data.marshal())
//	return string(bytes), err
//}
