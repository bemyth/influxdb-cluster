package coordinator

import (
	"context"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"io"
	"time"
)

// IteratorCreator is an interface that combines mapping fields and creating iterators.
type IteratorCreator interface {
	query.IteratorCreator
	influxql.FieldMapper
	io.Closer
}

//cluster shard mapper 面向查询语句；返回构建查询计划时需要的信息，由于这些信息不确定在哪一个shard上，所以需要在一定范围类的所有节点执行
//而我们可以根据shard中owner来对其中一个节点执行就可以了。
//在没有副本的情况下，一个shard中只有一个拥有者，而有副本时，可能多个节点存的是同一个shard，在查询信息时，我们只需要在一个节点上
//执行就可以，更特殊的我们可以对本地节点走特殊通道，但在此为了一致性，暂时全部采用remote的形式
type ClusterShardMapper struct {
	MetaClient interface {
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
		GetOnlineNodeByShardID(si meta.ShardInfo) (nodeID uint64, err error)
	}
	ProxyStore interface {
		ShardGroup(map[uint64][]uint64) tsdb.ShardGroup
	}
}

func (c *ClusterShardMapper) MapShards(sources influxql.Sources, t influxql.TimeRange, opt query.SelectOptions) (query.ShardGroup, error) {
	a := &ClusterShardMapping{
		ShardMap: make(map[Source]tsdb.ShardGroup),
	}
	tmin := time.Unix(0, t.MinTimeNano())
	tmax := time.Unix(0, t.MaxTimeNano())
	if err := c.mapShards(a, sources, tmin, tmax); err != nil {
		return nil, err
	}
	a.MinTime, a.MaxTime = tmin, tmax
	return a, nil
}

func (c *ClusterShardMapper) mapShards(a *ClusterShardMapping, sources influxql.Sources, tmin, tmax time.Time) error {
	for _, s := range sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			{
				source := Source{
					Database:        s.Database,
					RetentionPolicy: s.RetentionPolicy,
				}
				if _, ok := a.ShardMap[source]; !ok {
					//var NodeIDByShardIDs map[uint64][]uint64
					NodeIDByShardIDs := make(map[uint64][]uint64) //key:nodeID  value: shardIDs
					groups, err := c.MetaClient.ShardGroupsByTimeRange(s.Database, s.RetentionPolicy, tmin, tmax)
					if err != nil {
						return err
					}
					if len(groups) == 0 {
						a.ShardMap[source] = nil
						continue
					}
					for _, g := range groups {
						for _, shardInfo := range g.Shards {
							//副本中随机挑选一个节点 //从副本中挑选一个活跃的节点
							nodeID, err := c.MetaClient.GetOnlineNodeByShardID(shardInfo)
							if err != nil {
								return err
							}
							//nodeID := shardInfo.Owners[rand.Int()%len(shardInfo.Owners)].NodeID
							if _, ok2 := NodeIDByShardIDs[nodeID]; ok2 { //已存在该节点，增加shardID
								NodeIDByShardIDs[nodeID] = append(NodeIDByShardIDs[nodeID], shardInfo.ID)
							} else { //创建ID
								NodeIDByShardIDs[nodeID] = []uint64{shardInfo.ID}
							}
						}
					}
					a.ShardMap[source] = c.ProxyStore.ShardGroup(NodeIDByShardIDs)
				}
			}
		case *influxql.SubQuery:
			{
				if err := c.mapShards(a, s.Statement.Sources, tmin, tmax); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type ClusterShardMapping struct {
	ShardMap map[Source]tsdb.ShardGroup
	MinTime  time.Time
	MaxTime  time.Time
}

func (a *ClusterShardMapping) FieldDimensions(m *influxql.Measurement) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return
	}

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	var measurements []string
	if m.Regex != nil {
		measurements = sg.MeasurementsByRegex(m.Regex.Val)
	} else {
		measurements = []string{m.Name}
	}

	f, d, err := sg.FieldDimensions(measurements)
	if err != nil {
		return nil, nil, err
	}
	for k, typ := range f {
		fields[k] = typ
	}
	for k := range d {
		dimensions[k] = struct{}{}
	}
	return
}

func (a *ClusterShardMapping) MapType(m *influxql.Measurement, field string) influxql.DataType {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}
	sg := a.ShardMap[source]
	if sg == nil {
		return influxql.Unknown
	}

	var names []string
	if m.Regex != nil {
		names = sg.MeasurementsByRegex(m.Regex.Val)
	} else {
		names = []string{m.Name}
	}
	var typ influxql.DataType
	for _, name := range names {
		if m.SystemIterator != "" {
			name = m.SystemIterator
		}
		t := sg.MapType(name, field)
		if typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (a *ClusterShardMapping) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}
	sg := a.ShardMap[source]
	if sg == nil {
		return nil, nil
	}
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}

	if m.Regex != nil {
		measurements := sg.MeasurementsByRegex(m.Regex.Val)
		inputs := make([]query.Iterator, 0, len(measurements))
		if err := func() error {
			for _, measurement := range measurements {
				mm := m.Clone()
				mm.Name = measurement
				input, err := sg.CreateIterator(ctx, mm, opt)
				if err != nil {
					return err
				}
				inputs = append(inputs, input)
			}
			return nil
		}(); err != nil {
			query.Iterators(inputs).Close()
			return nil, err
		}
	}
	return sg.CreateIterator(ctx, m, opt)
}
func (a *ClusterShardMapping) IteratorCost(m *influxql.Measurement, opt query.IteratorOptions) (query.IteratorCost, error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	sg := a.ShardMap[source]
	if sg == nil {
		return query.IteratorCost{}, nil
	}

	// Override the time constraints if they don't match each other.
	if !a.MinTime.IsZero() && opt.StartTime < a.MinTime.UnixNano() {
		opt.StartTime = a.MinTime.UnixNano()
	}
	if !a.MaxTime.IsZero() && opt.EndTime > a.MaxTime.UnixNano() {
		opt.EndTime = a.MaxTime.UnixNano()
	}
	if m.Regex != nil {
		var costs query.IteratorCost
		measurements := sg.MeasurementsByRegex(m.Regex.Val)
		for _, measurement := range measurements {
			cost, err := sg.IteratorCost(measurement, opt)
			if err != nil {
				return query.IteratorCost{}, err
			}
			costs = costs.Combine(cost)
		}
		return costs, nil
	}
	return sg.IteratorCost(m.Name, opt)
}

func (a *ClusterShardMapping) Close() error {
	a.ShardMap = nil
	return nil
}

type Source struct {
	Database        string
	RetentionPolicy string
}
