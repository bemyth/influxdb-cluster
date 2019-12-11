package coordinator

import (
	"fmt"
	"github.com/influxdata/influxdb/coordinator/transmitter"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	NumberOfRetryWithDeleteError   = 3
	IntervalOfRetryWithDeleteError = 1 * time.Second
)

var (
	ErrorNoRpcAddrInThisShard = errors.New("no rpcAddr in this shard")
)

type ProxyStore interface {
	MeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error)
	WriteToShard(shardID uint64, ownerID uint64, database string, retentionPolicy string, points []models.Point) error
	DeleteSeries(database string, sources influxql.Sources, expr influxql.Expr) error
	DeleteMeasurement(database, name string) error
	DeleteRetentionPolicy(database, name string) error
	DeleteShard(shardID uint64) error
	DeleteDatabase(database string) error
	TagKeys(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValues(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
}

/*
1.集群权限控制    ✔
2.集群信息监控
3.重新组织更加清晰的代码结构
4.集群cachedata同步方式有待完善
5.所有集群化的语句均未做异常处理，需要完善的方案来解决
*/

//Cluster implement ProxyStore
type Cluster struct {
	MetaClient  MetaClient
	TSDBStore   TSDBStore
	Transmitter transmitter.Transmitter
	logger      *zap.Logger
}

func NewCluster() *Cluster {
	return &Cluster{}
}

func (c *Cluster) WithLogger(log *zap.Logger) {
	c.logger = log.With(zap.String("service", "cluster"))
}

func (c *Cluster) ShardGroup(m map[uint64][]uint64) tsdb.ShardGroup {
	a := &ClusterShard{
		NodeIDByShardIds: m,
	}
	a.Transmitter = c.Transmitter
	a.MetaClient = c.MetaClient
	a.logger = c.logger
	return a
}

func (c *Cluster) WriteToShard(shardID uint64, ownerID uint64, database string, retentionPolicy string, points []models.Point) error {

	target, err := c.MetaClient.GetRPCHostByNodeID(ownerID)
	if err != nil {
		return err
	}
	return c.Transmitter.TaskWriteToShard(target, shardID, database, retentionPolicy, points)
}

//异步模式 获取measurement名称
//若节点失败，则全局失败
func (c *Cluster) MeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error) {
	allName := [][]byte{}
	//使用map过滤重复元素
	var mutex sync.RWMutex
	mname := make(map[string]struct{})
	hosts, err1 := c.MetaClient.GetAllRPCHosts()
	if err1 != nil {
		return nil, err1
	}
	//协程完成状态
	var consterr error
	setErr := func(err error) {
		mutex.Lock()
		defer mutex.Unlock()
		if consterr == nil {
			consterr = err
		}
	}
	var wg sync.WaitGroup
	for _, host := range hosts {
		mutex.RLock()
		if consterr != nil {
			mutex.RUnlock()
			break
		}
		mutex.RUnlock()
		wg.Add(1)
		go func(target string) (err error) {
			defer func() {
				setErr(err)
				wg.Done()
			}()
			names, err := c.Transmitter.TaskMeasurementNames(target, auth, database, cond)
			if err != nil {
				err = errors.New(fmt.Sprintf("request to %s error because %s", target, err.Error()))
				return
			}
			mutex.Lock()
			for _, name := range names {
				if _, ok := mname[string(name)]; !ok {
					mname[string(name)] = struct{}{}
					allName = append(allName, name)
				}
			}
			mutex.Unlock()
			return
		}(host)
	}
	wg.Wait()
	return allName, consterr
}

//顺序执行删除数据库
func (c *Cluster) DeleteDatabase(database string) error {
	hosts, err := c.MetaClient.GetAllRPCHosts()
	if err != nil {
		return err
	}
	for _, host := range hosts {
		for i := 1; i <= NumberOfRetryWithDeleteError; i++ {
			if err := c.Transmitter.TaskDeleteDatabase(host, database); err != nil {
				c.logger.Warn(fmt.Sprintf("delete database %s in %s error & ready to retry %d", database, host, i))
				if i == NumberOfRetryWithDeleteError {
					err = errors.New(fmt.Sprintf("delete to %s error because %s", host, err.Error()))
					return err
				}
				time.Sleep(IntervalOfRetryWithDeleteError)
			} else {
				break
			}
		}
		c.logger.Info(fmt.Sprintf("delete database %s in %s success", database, host))
	}
	c.logger.Info(fmt.Sprintf("delete database %s success", database))
	return nil
}

////异步模式 删除数据库
//func (c *Cluster) DeleteDatabase(database string) error {
//	hosts, err := c.MetaClient.GetAllRPCHosts()
//	if err != nil {
//		return err
//	}
//	var wg sync.WaitGroup
//	for _, host := range hosts {
//		wg.Add(1)
//		go func(target string) {
//			retry := 0
//			defer wg.Done()
//			for true {
//
//				retry++
//				if err := c.Transmitter.TaskDeleteDatabase(target, database); err != nil {
//					c.logger.Warn(fmt.Sprintf("delete database %s in %s error & ready to retry %d", database, target, retry))
//					time.Sleep(100 * time.Millisecond)
//					continue
//				}
//				c.logger.Info(fmt.Sprintf("delete database %s in %s success", database, target))
//				break
//			}
//		}(host)
//	}
//	wg.Wait()
//	c.logger.Info(fmt.Sprintf("delete database %s success", database))
//	return nil
//}

//顺序方式删除series
func (c *Cluster) DeleteSeries(database string, sources influxql.Sources, expr influxql.Expr) error {
	//考虑series的删除操作，以及tsm存储系统，没必要先查询这个series存在的节点，再去删除，直接执行就好了，效率更高
	hosts, err := c.MetaClient.GetAllRPCHosts()
	if err != nil {
		return err
	}
	sexpr, err := influxql.EncodeCond(expr)
	if err != nil {
		return err
	}
	for _, host := range hosts {
		for i := 1; i <= NumberOfRetryWithDeleteError; i++ {
			if err := c.Transmitter.TaskDeleteSeries(host, database, sources, expr); err != nil {
				c.logger.Warn(fmt.Sprintf("delete series %s in %s error & ready to retry %d", sexpr, host, i))
				if i == NumberOfRetryWithDeleteError {
					err = errors.New(fmt.Sprintf("delete to %s error because %s", host, err.Error()))
					return err
				}
				time.Sleep(IntervalOfRetryWithDeleteError)
			} else {
				break
			}
		}
		c.logger.Info(fmt.Sprintf("delete series %s in %s success", sexpr, host))
	}
	c.logger.Info(fmt.Sprintf("delete series %s success", sexpr))
	return err
}

//异步模式 删除series
//func (c *Cluster) DeleteSeries(database string, sources influxql.Sources, expr influxql.Expr) error {
//	//考虑series的删除操作，以及tsm存储系统的特殊性，没必要先查询这个series存在的节点，再去删除，直接执行就好了，效率更高
//	hosts, err := c.MetaClient.GetAllRPCHosts()
//	if err != nil {
//		return err
//	}
//	sexpr, err := influxql.EncodeCond(expr)
//	if err != nil {
//		return err
//	}
//	var wg sync.WaitGroup
//	for _, host := range hosts {
//		wg.Add(1)
//		go func(target string) {
//			retry := 0
//			defer wg.Done()
//			for true {
//
//				retry++
//				if err := c.Transmitter.TaskDeleteSeries(target, database, sources, expr); err != nil {
//					c.logger.Warn(fmt.Sprintf("delete series %s in %s error & ready to retry %d", sexpr, target, retry))
//					time.Sleep(100 * time.Millisecond)
//					continue
//				}
//				c.logger.Info(fmt.Sprintf("delete series %s in %s success", sexpr, target))
//				break
//			}
//		}(host)
//	}
//	wg.Wait()
//	c.logger.Info(fmt.Sprintf("delete series %s success", sexpr))
//	return err
//}

//顺序方式删除measurement
func (c *Cluster) DeleteMeasurement(database, name string) error {
	hosts, err := c.MetaClient.GetAllRPCHosts()
	if err != nil {
		return err
	}
	for _, host := range hosts {
		for i := 1; i <= NumberOfRetryWithDeleteError; i++ {
			if err := c.Transmitter.TaskDeleteMeasurement(host, database, name); err != nil {
				c.logger.Warn(fmt.Sprintf("delete measurement %s in %s error & ready to retry %d", name, host, i))
				if i == NumberOfRetryWithDeleteError {
					err = errors.New(fmt.Sprintf("delete to %s error because %s", host, err.Error()))
					return err
				}
				time.Sleep(IntervalOfRetryWithDeleteError)
			} else {
				break
			}
		}
		c.logger.Info(fmt.Sprintf("delete measurement %s in %s success", name, host))
	}
	c.logger.Info(fmt.Sprintf("delete measurement %s success", name))
	return nil
}

//异步模式 删除measurement
//func (c *Cluster) DeleteMeasurement(database, name string) error {
//	hosts, err := c.MetaClient.GetAllRPCHosts()
//	if err != nil {
//		return err
//	}
//	var wg sync.WaitGroup
//	for _, host := range hosts {
//		wg.Add(1)
//		go func(target string) {
//			defer wg.Done()
//			retry := 0
//			for true {
//				retry++
//				if err := c.Transmitter.TaskDeleteMeasurement(target, database, name); err != nil {
//					c.logger.Warn(fmt.Sprintf("delete measurement %s in %s error & ready to retry %d", name, target, retry))
//					continue
//				}
//				c.logger.Info(fmt.Sprintf("delete measurement %s in %s success", name, target))
//				break
//			}
//		}(host)
//	}
//	wg.Wait()
//	c.logger.Info(fmt.Sprintf("delete measurement %s success", name))
//	return nil
//}

//顺序方式删除保留策略
func (c *Cluster) DeleteRetentionPolicy(database, name string) error {
	hosts, err := c.MetaClient.GetAllRPCHosts()
	if err != nil {
		return err
	}
	for _, host := range hosts {
		for i := 1; i <= NumberOfRetryWithDeleteError; i++ {
			if err := c.Transmitter.TaskDeleteRetentionPolicy(host, database, name); err != nil {
				c.logger.Warn(fmt.Sprintf("delete retention policy %s in %s error & ready to retry %d", name, host, i))
				if i == NumberOfRetryWithDeleteError {
					err = errors.New(fmt.Sprintf("delete to %s error because %s", host, err.Error()))
					return err
				}
			} else {
				break
			}
		}
		c.logger.Warn(fmt.Sprintf("delete retention policy %s in %s success", name, host))
	}
	c.logger.Warn(fmt.Sprintf("delete retention policy %s success", name))
	return err
}

//异步模式 删除保留策略
//func (c *Cluster) DeleteRetentionPolicy(database, name string) error {
//	hosts, err := c.MetaClient.GetAllRPCHosts()
//	if err != nil {
//		return err
//	}
//	var wg sync.WaitGroup
//	for _, host := range hosts {
//		wg.Add(1)
//		go func(target string) {
//			retry := 0
//			defer wg.Done()
//			for true {
//
//				retry++
//				if err := c.Transmitter.TaskDeleteRetentionPolicy(target, database, name); err != nil {
//					c.logger.Warn(fmt.Sprintf("delete retention policy %s in %s error & ready to retry %d", name, target, retry))
//					continue
//				}
//				c.logger.Warn(fmt.Sprintf("delete retention policy %s in %s success", name, target))
//				break
//			}
//		}(host)
//	}
//	wg.Wait()
//	c.logger.Warn(fmt.Sprintf("delete retention policy %s success", name))
//	return err
//}

//顺序方式删除shard
func (c *Cluster) DeleteShard(shardID uint64) error {
	hosts, err := c.MetaClient.GetRPCHostsByShardID(shardID)
	if err != nil {
		return err
	}
	for _, host := range hosts {
		for i := 1; i <= NumberOfRetryWithDeleteError; i++ {
			if err := c.Transmitter.TaskDeleteShard(host, shardID); err != nil {
				c.logger.Warn(fmt.Sprintf("delete shard %d in %s error & ready to retry %d", shardID, host, i))
				if i == NumberOfRetryWithDeleteError {
					err = errors.New(fmt.Sprintf("delete to %s error because %s", host, err.Error()))
					return err
				}
			} else {
				break
			}
		}
		c.logger.Info(fmt.Sprintf("delete shard %d in %s success", shardID, host))
	}
	c.logger.Info(fmt.Sprintf("delete shard %d success", shardID))
	return nil
}

//异步模式 删除shard
//func (c *Cluster) DeleteShard(shardID uint64) error {
//	hosts, err := c.MetaClient.GetRPCHostsByShardID(shardID)
//	if err != nil {
//		return err
//	}
//	var wg sync.WaitGroup
//	for _, host := range hosts {
//		wg.Add(1)
//		go func(target string) {
//			defer wg.Done()
//			retry := 0
//			for true {
//				retry++
//				if err := c.Transmitter.TaskDeleteShard(target, shardID); err != nil {
//					c.logger.Warn(fmt.Sprintf("delete shard %d in %s error & ready to retry %d", shardID, target, retry))
//					continue
//				}
//				c.logger.Info(fmt.Sprintf("delete shard %d in %s success", shardID, target))
//				break
//			}
//		}(host)
//	}
//	wg.Wait()
//	c.logger.Info(fmt.Sprintf("delete shard %d success", shardID))
//	return nil
//}

//异步模式 show tag keys
func (c *Cluster) TagKeys(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error) {
	//对于基于一批shard的查询，由于这些shard分布在不同的节点上，所以需要对这批shard根据节点建立分批map[nodeid][]shardid
	//此时有两种情况，如果shard1，shard2，都在node4，node5上有备份，那么我们是将shard1和shard2分别分发到4，5还是集中在4或者5.
	//此处我选择分散在4，5。因为当shard数量较多时(实际情况总是很多)，并不会有空闲的节点。集中减少网络开销优势也就没了
	nodeByShardIDs := make(map[string][]uint64)
	for _, shardID := range shardIDs {
		host, err := c.MetaClient.GetRPCHostByShardID(shardID)
		if err != nil {
			return nil, err
		}
		nodeByShardIDs[host] = append(nodeByShardIDs[host], shardID)
	}
	//利用map过滤重复元素（measurement，tagkey）
	var mutex sync.RWMutex
	rstTagKeys := make(map[string]map[string]struct{})

	var consterr error
	setErr := func(err error) {
		mutex.Lock()
		defer mutex.Unlock()
		if consterr == nil {
			consterr = err
		}
	}
	var wg sync.WaitGroup
	for host, si := range nodeByShardIDs {
		mutex.RLock()
		if consterr != nil {
			mutex.RUnlock()
			break
		}
		mutex.RUnlock()
		wg.Add(1)
		go func(target string, shardIDs []uint64) (err error) {
			defer func() {
				setErr(err)
				wg.Done()
			}()
			tagKeys, err := c.Transmitter.TaskTagKeys(target, auth, shardIDs, cond)
			if err != nil {
				err = errors.New(fmt.Sprintf("request to %s error because %s", target, err.Error()))
				return
			}
			mutex.Lock()
			for m, tks := range tagKeys {
				if _, ok := rstTagKeys[m]; !ok {
					rstTagKeys[m] = make(map[string]struct{})
				}
				for _, tk := range tks {
					rstTagKeys[m][tk] = struct{}{}
				}
			}
			mutex.Unlock()
			return
		}(host, si)
	}
	wg.Wait()

	//格式化结果
	//最终返回的结果集
	rst := make([]tsdb.TagKeys, 0)
	for m, tks := range rstTagKeys {
		tmp := tsdb.TagKeys{
			Measurement: m,
		}
		for tk, _ := range tks {
			tmp.Keys = append(tmp.Keys, tk)
		}
		rst = append(rst, tmp)
	}
	return rst, consterr
}

func (c *Cluster) TagValues(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error) {
	nodeByShardIDs := make(map[string][]uint64)
	for _, shardID := range shardIDs {
		host, err := c.MetaClient.GetRPCHostByShardID(shardID)
		if err != nil {
			return nil, err
		}
		nodeByShardIDs[host] = append(nodeByShardIDs[host], shardID)
	}

	//利用map过滤重复元素（measurement，tagkey）
	var mutex sync.RWMutex
	rstTagValues := make(map[string]map[string]map[string]struct{})

	var consterr error
	setErr := func(err error) {
		mutex.Lock()
		defer mutex.Unlock()
		if consterr == nil {
			consterr = err
		}
	}

	var wg sync.WaitGroup
	for host, si := range nodeByShardIDs {
		mutex.RLock()
		if consterr != nil {
			mutex.RUnlock()
			break
		}
		mutex.RUnlock()
		wg.Add(1)
		go func(target string, shardIDs []uint64) (err error) {
			defer func() {
				setErr(err)
				wg.Done()
			}()
			tagValues, err := c.Transmitter.TaskTagValues(target, auth, shardIDs, cond)
			if err != nil {
				err = errors.New(fmt.Sprintf("request to %s error because %s", target, err.Error()))
				return
			}

			mutex.Lock()
			for m, tkvs := range tagValues {
				if _, ok := rstTagValues[m]; !ok {
					rstTagValues[m] = make(map[string]map[string]struct{})
				}
				for tk, tvs := range tkvs {
					if _, ok := rstTagValues[m][tk]; !ok {
						rstTagValues[m][tk] = make(map[string]struct{})
					}
					for _, tv := range tvs {
						rstTagValues[m][tk][tv] = struct{}{}
					}
				}
			}
			mutex.Unlock()
			return
		}(host, si)
	}
	wg.Wait()

	//格式化结果
	//最终返回的结果集
	rst := make([]tsdb.TagValues, 0)
	for m, tkvs := range rstTagValues {
		tmpmkv := tsdb.TagValues{
			Measurement: m,
		}
		for tk, tvs := range tkvs {
			for tv, _ := range tvs {
				tmpkv := tsdb.KeyValue{
					Key:   tk,
					Value: tv,
				}
				tmpmkv.Values = append(tmpmkv.Values, tmpkv)
			}
		}
		rst = append(rst, tmpmkv)
	}
	return rst, consterr
}
