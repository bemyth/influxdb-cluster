package transmitter

import (
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"regexp"
)

//Interface to provide services to  pointWrite and statementExecutor
type Transmitter interface {
	TaskWriteToShard(target string, shardID uint64, database, retentionPolicy string, points []models.Point) error

	TaskMeasurementNamesByRegex(target string, sharIDs []uint64, re *regexp.Regexp) ([]string, error)
	TaskMapType(target string, shardIDs []uint64, measurement string, fields string) (influxql.DataType, error)
	TaskFieldDimensions(target string, shardIDs []uint64, measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	TaskCreateIterator(target string, shardIDs []uint64, measurement *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error)

	TaskMeasurementNames(target string, auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error)
	TaskDeleteDatabase(target string, database string) error
	TaskDeleteSeries(target string, database string, sources influxql.Sources, expr influxql.Expr) error
	TaskDeleteMeasurement(target, database, name string) error
	TaskDeleteRetentionPolicy(target, database, name string) error
	TaskDeleteShard(target string, shardID uint64) error
	//result map[measurement][]key
	TaskTagKeys(target string, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) (map[string][]string, error)
	//result map[measurement]map[key][]value
	TaskTagValues(target string, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) (map[string]map[string][]string, error)
	TaskIteratorCost(target string, shardIDs []uint64, measurement string, opt query.IteratorOptions) (query.IteratorCost, error)

	JoinToCluster(target, raftID, raftAddr string) error
	ApplyToLeader(target string, b []byte) error
}
