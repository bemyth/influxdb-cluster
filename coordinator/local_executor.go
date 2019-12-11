package coordinator

import (
	"context"
	"fmt"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"regexp"
	"strconv"
)

type LocalExecutor interface {
	LocalMeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error)

	LocalMeasurementNamesByRegex(ids []uint64, regex *regexp.Regexp) ([]string, error)
	LocalMapType(ids []uint64, measurement, field string) (influxql.DataType, error)
	LocalFieldDimensions(ids []uint64, measurements []string) (map[string]influxql.DataType, map[string]struct{}, error)
	LocalCreateIterator(ids []uint64, measurement *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error)
	LocalIteratorCost(ids []uint64, measurement string, opt query.IteratorOptions) (query.IteratorCost, error)
	LocalDeleteDatabase(database string) error
	LocalDeleteSeries(database string, sources influxql.Sources, expr influxql.Expr) error
	LocalDeleteMeasurement(database, name string) error
	LocalDeleteRetentionPolicy(database, name string) error
	LocalDeleteShard(shardID uint64) error
	LocalTagKeys(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	LocalTagValues(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
}

func (c *Cluster) LocalMeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error) {
	scond, err := influxql.EncodeCond(cond)
	if err != nil {
		return nil, err
	}
	stmt := fmt.Sprintf("database = %s,cond = %s", database, scond)
	c.logger.Info(stmt, zap.String("function", "LocalMeasurementNames"))
	if database == "" {
		return nil, ErrDatabaseNameRequired
	}
	names, err := c.TSDBStore.MeasurementNames(auth, database, cond)
	if err != nil {
		return nil, err
	}
	return names, nil
}

//select statement 调用
func (c *Cluster) LocalMeasurementNamesByRegex(ids []uint64, regex *regexp.Regexp) ([]string, error) {
	sg := c.TSDBStore.ShardGroup(ids)
	names := sg.MeasurementsByRegex(regex)
	return names, nil
}

//select statement 调用
func (c *Cluster) LocalMapType(ids []uint64, measurement, field string) (influxql.DataType, error) {
	stmt := fmt.Sprintf("measurement = %s,field = %s", measurement, field)
	c.logger.Info(stmt, zap.String("function", "LocalMapType"))
	sg := c.TSDBStore.ShardGroup(ids)
	dtype := sg.MapType(measurement, field)
	return dtype, nil
}

//select statement 调用
func (c *Cluster) LocalFieldDimensions(ids []uint64, measurements []string) (map[string]influxql.DataType, map[string]struct{}, error) {
	stmt := fmt.Sprintf("measurement = %s", measurements[0])
	c.logger.Info(stmt, zap.String("function", "LocalFieldDimensions"))
	sg := c.TSDBStore.ShardGroup(ids)
	f, d, err := sg.FieldDimensions(measurements)
	if err != nil {
		return nil, nil, err
	}
	return f, d, err
}

//select statement 调用
func (c *Cluster) LocalCreateIterator(ids []uint64, m *influxql.Measurement, opt query.IteratorOptions) (itr query.Iterator, err error) {
	saux := make([]string, 0)
	if opt.Aux != nil {
		for _, aux := range opt.Aux {
			saux = append(saux, aux.String())
		}
	}
	scond, err := influxql.EncodeCond(opt.Expr)
	if err != nil {
		return nil, err
	}
	stmt := fmt.Sprintf("measurement = %s,cond = %s,aux = %s", m.Name, scond, saux)
	c.logger.Info(stmt, zap.String("funtion", "LocalCreateIterator"))
	sg := c.TSDBStore.ShardGroup(ids)
	if m.Regex != nil {
		measurements := sg.MeasurementsByRegex(m.Regex.Val)
		inputs := make([]query.Iterator, 0, len(measurements))
		if err := func() error {
			// Create a Measurement for each returned matching measurement value
			// from the regex.
			for _, measurement := range measurements {
				mm := m.Clone()
				mm.Name = measurement // Set the name to this matching regex value.
				input, err := sg.CreateIterator(context.Background(), mm, opt)
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

		itr, err = query.Iterators(inputs).Merge(opt)
		if err != nil {
			return nil, err
		}
	}
	itr, err = sg.CreateIterator(context.Background(), m, opt)
	if err != nil {
		return nil, err
	}
	return itr, nil
}
func (c *Cluster) LocalDeleteDatabase(database string) error {
	stmt := fmt.Sprintf("database = %s", database)
	c.logger.Info(stmt, zap.String("function", "LocalDeleteDatabase"))
	return c.TSDBStore.DeleteDatabase(database)
}
func (c *Cluster) LocalDeleteSeries(database string, sources influxql.Sources, expr influxql.Expr) error {
	scond, err := influxql.EncodeCond(expr)
	if err != nil {
		return err
	}
	stmt := fmt.Sprintf("database = %s,sources = %s, expr = %s", database, sources.String(), scond)
	c.logger.Info(stmt, zap.String("function", "LocalDeleteSeries"))
	return c.TSDBStore.DeleteSeries(database, sources, expr)
}
func (c *Cluster) LocalDeleteMeasurement(database, name string) error {
	stmt := fmt.Sprintf("database = %s,measurement = %s", database, name)
	c.logger.Info(stmt, zap.String("function", "LocalDeleteMeasurement"))
	return c.TSDBStore.DeleteMeasurement(database, name)
}
func (c *Cluster) LocalDeleteRetentionPolicy(database, name string) error {
	stmt := fmt.Sprintf("database = %s,retentionPolicy = %s", database, name)
	c.logger.Info(stmt, zap.String("function", "LocalDeleteRetentionPolicy"))
	return c.TSDBStore.DeleteRetentionPolicy(database, name)
}
func (c *Cluster) LocalDeleteShard(shardID uint64) error {
	stmt := fmt.Sprintf("shardID = %s", strconv.Itoa(int(shardID)))
	c.logger.Info(stmt, zap.String("function", "LocalDeleteShard"))
	return c.TSDBStore.DeleteShard(shardID)
}
func (c *Cluster) LocalTagKeys(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error) {
	scond, err := influxql.EncodeCond(cond)
	if err != nil {
		return nil, err
	}
	stmt := fmt.Sprintf("cond = %s", scond)
	c.logger.Info(stmt, zap.String("function", "LocalTagKeys"))
	return c.TSDBStore.TagKeys(auth, shardIDs, cond)
}
func (c *Cluster) LocalTagValues(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error) {
	scond, err := influxql.EncodeCond(cond)
	if err != nil {
		return nil, err
	}
	stmt := fmt.Sprintf("cond = %s", scond)
	c.logger.Info(stmt, zap.String("function", "LocalTagValues"))
	return c.TSDBStore.TagValues(auth, shardIDs, cond)
}
func (c *Cluster) LocalIteratorCost(shardIDs []uint64, measurement string, opt query.IteratorOptions) (query.IteratorCost, error) {
	sg := c.TSDBStore.ShardGroup(shardIDs)
	return sg.IteratorCost(measurement, opt)
}
