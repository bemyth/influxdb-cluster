package meta

//
//import (
//	"github.com/influxdata/influxdb/models"
//	"github.com/influxdata/influxdb/query"
//	"github.com/influxdata/influxql"
//	"github.com/influxdata/influxdb/tsdb"
//	"regexp"
//	"context"
//)
//
//type Server interface {
//	WriteToShard(shardID uint64,ownerID uint64,database string,retentionPolicy string,points []models.Point) error
//
//	MeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error)
//}
//
//func (c *Client)WriteToShard(shardID uint64,ownerID uint64,database string,retentionPolicy string,points []models.Point) error{
//	var target string
//	for _,node := range c.cacheData.DataNodes{
//		if ownerID == node.ID{
//			target = node.RPCHost
//		}
//	}
//	return  c.Transmitter.TaskWriteToShard(target,shardID,database,retentionPolicy,points)
//
//}
//func (c *Client)MeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) (allName [][]byte, err error){
//
//	var scond string
//	if cond ==nil{
//		scond =""
//	}
//	allName= [][]byte{}
//	for _,node := range c.cacheData.DataNodes{
//		//target := node.RPCHost
//		names,err := c.Transmitter.TaskMeasurementNames(node.RPCHost,database,scond)
//		if err != nil{
//			continue
//			//return destnames,err
//		}
//		for _,name := range names{
//			allName = append(allName, name)
//		}
//	}
//	return
//}
//
//func (c *Client)ShardGroup(si map[uint64][]uint64) tsdb.ShardGroup{
//
//	return &ClusterShard{
//		shardIDsByNodeIDs:si,
//	}
//}
//
//type ClusterShard struct {
//
//	shardIDsByNodeIDs map[uint64][]uint64
//
//
//	ClusterInfo interface{
//
//	}
//
//
//	//Data transmission between nodes
//	Transmitter interface{
//		MeasurementNamesByRegex(NodeIDre *regexp.Regexp) ([][][]byte,error)
//		TaskWriteToShard(target string,shardID uint64,database string,retentionPolicy string,points []models.Point) error
//		TaskMeasurementNames(target string,database string,cond string) ([][]byte,error)
//	}
//
//}
//
//func (c *ClusterShard)MeasurementsByRegex(re *regexp.Regexp) []string{
//	var m map[string]struct{}
//	for NodeID, shardids := range c.shardIDsByNodeIDs{
//		names,err := c.Transmitter.MeasurementNamesByRegex(re)
//
//	}
//	return []string{""}
//}
//func (c *ClusterShard)FieldKeysByMeasurement(name []byte) []string{
//	return []string{""}
//}
//func (c *ClusterShard)FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error){
//
//}
//
//func (c *ClusterShard)MapType(measurement, field string) influxql.DataType{
//
//}
//func (c *ClusterShard)CreateIterator(ctx context.Context, measurement *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error){
//
//}
//func (c *ClusterShard)IteratorCost(measurement string, opt query.IteratorOptions) (query.IteratorCost, error){
//
//}
//func (c *ClusterShard)ExpandSources(sources influxql.Sources) (influxql.Sources, error){
//
//}
