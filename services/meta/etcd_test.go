package meta

//
//import (
//	"testing"
//	"github.com/coreos/etcd/clientv3"
//	"context"
//	"github.com/coreos/etcd/mvcc/mvccpb"
//	"fmt"
//)
//
//func TestWathch(t *testing.T) {
//
//	client, err := clientv3.New(clientv3.Config{
//		Endpoints:[]string{"172.16.72.38:2379"},
//		DialTimeout:DefaultDialTimeOut,
//		})
//	if err != nil{
//		return
//	}
//	watch := clientv3.NewWatcher(client)
//
//	watchChan := watch.Watch(context.TODO(),"/test/agent/1")
//	for watchResp := range watchChan{
//		for _,event := range watchResp.Events{
//			switch event.Type {
//			case mvccpb.PUT:{
//				fmt.Println("put ",event.Kv)
//			}
//			case mvccpb.DELETE:{
//				fmt.Println("delete ",event.Kv)
//			}
//			}
//		}
//	}
//
//}
//
//func TestPut(t *testing.T){
//	client, err := clientv3.New(clientv3.Config{
//		Endpoints:[]string{"172.16.72.38:2379"},
//		DialTimeout:DefaultDialTimeOut,
//	})
//	if err != nil{
//		return
//	}
//	kv := clientv3.NewKV(client)
//	kv.Put(context.TODO(),"/test/agent/1","3")
//}
//
//func TestGet(t *testing.T){
//	client, err := clientv3.New(clientv3.Config{
//		Endpoints:[]string{"172.16.72.38:2379"},
//		DialTimeout:DefaultDialTimeOut,
//	})
//	if err != nil{
//		return
//	}
//	kv := clientv3.NewKV(client)
//	kv.Put(context.TODO(),"/test/agent/1","3")
//}
