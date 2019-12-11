package coordinator

import (
	"context"
	"github.com/influxdata/influxdb/coordinator/transmitter"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"regexp"
	"runtime"
	"sort"
	"sync"
)

type ClusterShard struct {
	NodeIDByShardIds map[uint64][]uint64

	MetaClient  MetaClient
	Transmitter transmitter.Transmitter

	logger *zap.Logger

	TaskIndex uint64
}

func (c *ClusterShard) MeasurementsByRegex(re *regexp.Regexp) []string {

	var mutex sync.Mutex
	//归并结果，去除重复元
	m := make(map[string]struct{})

	var wg sync.WaitGroup
	for nodeID, shardIDs := range c.NodeIDByShardIds {
		wg.Add(1)
		go func(ni uint64, sis []uint64) {
			defer func() {
				wg.Done()
			}()
			target, err := c.MetaClient.GetRPCHostByNodeID(ni)
			if err != nil {
				return
			}
			names, err := c.Transmitter.TaskMeasurementNamesByRegex(target, sis, re)
			if err != nil {
				return
			}
			mutex.Lock()
			for _, name := range names {
				m[name] = struct{}{}
			}
			mutex.Unlock()
			return
		}(nodeID, shardIDs)
	}
	wg.Wait()
	//格式化结果
	if len(m) == 0 {
		return nil
	}
	names := make([]string, 0, len(m))
	for key := range m {
		names = append(names, key)
	}
	sort.Strings(names)
	return names
}

func (c *ClusterShard) FieldKeysByMeasurement(name []byte) []string {
	return []string{""}
}

func (c *ClusterShard) FieldDimensions(measurements []string) (map[string]influxql.DataType, map[string]struct{}, error) {
	//格式化返回结果
	var mutex sync.RWMutex
	fields := make(map[string]influxql.DataType)
	dimensions := make(map[string]struct{})

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
	for nodeID, shardIDs := range c.NodeIDByShardIds {
		mutex.RLock()
		if consterr != nil {
			mutex.RUnlock()
			break
		}
		mutex.RUnlock()
		wg.Add(1)
		go func(ni uint64, sis []uint64) (err error) {
			defer func() {
				setErr(err)
				wg.Done()
			}()
			target, err := c.MetaClient.GetRPCHostByNodeID(ni)
			if err != nil {
				c.logger.Error(err.Error())
				return
			}
			f, d, err := c.Transmitter.TaskFieldDimensions(target, sis, measurements)
			if err != nil {
				c.logger.Error(err.Error())
				return
			}

			mutex.Lock()
			for k, typ := range f {
				if fields[k].LessThan(typ) {
					fields[k] = typ
				}
			}
			for k := range d {
				dimensions[k] = struct{}{}
			}
			mutex.Unlock()
			return
		}(nodeID, shardIDs)
	}
	wg.Wait()
	return fields, dimensions, consterr
}

//多线程
func (c *ClusterShard) MapType(measurement, field string) influxql.DataType {
	var mutex sync.Mutex
	var typ influxql.DataType

	var wg sync.WaitGroup
	for nodeID, shardIDs := range c.NodeIDByShardIds {
		wg.Add(1)
		go func(ni uint64, sis []uint64) {
			defer wg.Done()
			target, err := c.MetaClient.GetRPCHostByNodeID(nodeID)
			if err != nil {
				c.logger.Error(err.Error())
			}
			mutex.Lock()
			if t, err := c.Transmitter.TaskMapType(target, sis, measurement, field); err == nil && typ.LessThan(t) {
				typ = t
			}
			mutex.Unlock()
		}(nodeID, shardIDs)
	}
	wg.Wait()
	return typ
}

//1.并发写itrs，加锁保证正确性
//2.多协程报错，每个协程都将运行完成状态写入errC,正常完成写入nil
func (c *ClusterShard) CreateIterator(ctx context.Context, measurement *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	var mutex sync.RWMutex
	itrs := make([]query.Iterator, 0, len(c.NodeIDByShardIds))

	var consterr error
	setErr := func(err error) {
		mutex.Lock()
		defer mutex.Unlock()
		if consterr == nil {
			consterr = err
		}
	}

	var wg sync.WaitGroup
	for nodeID, shardIDs := range c.NodeIDByShardIds {
		//一旦有错误，就不必执行后边的循环
		mutex.RLock()
		if consterr != nil {
			mutex.RUnlock()
			break
		}
		mutex.RUnlock()
		wg.Add(1)
		go func(ni uint64, sis []uint64) (err error) {
			defer func() {
				setErr(err)
				wg.Done()
			}()
			host, err := c.MetaClient.GetRPCHostByNodeID(ni)
			if err != nil {
				c.logger.Error(err.Error())
				return
			}
			itr, err := c.Transmitter.TaskCreateIterator(host, sis, measurement, opt)
			if err != nil {
				if itr != nil {
					itr.Close()
				}
				c.logger.Error(err.Error())
				return
			}
			mutex.Lock()
			itrs = append(itrs, itr)
			mutex.Unlock()
			return
		}(nodeID, shardIDs)
	}
	wg.Wait()
	itr, consterr := query.Iterators(itrs).Merge(opt)
	return itr, consterr
}

func (c *ClusterShard) IteratorCost(measurement string, opt query.IteratorOptions) (query.IteratorCost, error) {
	var costs query.IteratorCost
	var costerr error
	var mu sync.RWMutex

	setErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if costerr == nil {
			costerr = err
		}
	}
	limit := limiter.NewFixed(runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	for nodeID, shardIDs := range c.NodeIDByShardIds {
		limit.Take()
		mu.RLock()
		if costerr != nil {
			mu.RUnlock()
			break
		}
		mu.RUnlock()
		wg.Add(1) //wg.add 应该在break之后
		go func(nid uint64, sids []uint64) (err error) {
			defer func() {
				setErr(err)
				limit.Release()
				wg.Done()
			}()
			target, err := c.MetaClient.GetRPCHostByNodeID(nid)
			if err != nil {
				return
			}
			cost, err := c.Transmitter.TaskIteratorCost(target, sids, measurement, opt)
			if err != nil {
				return
			}
			mu.Lock()
			costs = costs.Combine(cost)
			mu.Unlock()
			return
		}(nodeID, shardIDs)
	}
	wg.Wait()
	return costs, costerr
}

func (c *ClusterShard) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return nil, nil
}
