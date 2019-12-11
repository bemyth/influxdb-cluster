package coordinator_test

import (
	"fmt"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
	"regexp"
	"sync"
	"testing"
	"time"
)

type Transmitter struct {
}

func (t *Transmitter) TaskWriteToShard(target string, shardID uint64, database, retentionPolicy string, points []models.Point) error {
	return nil
}

func (t *Transmitter) TaskMeasurementNamesByRegex(target string, sharIDs []uint64, re *regexp.Regexp) ([]string, error) {
	return nil, nil
}
func (t *Transmitter) TaskMapType(target string, shardIDs []uint64, measurement string, fields string) (influxql.DataType, error) {
	return influxql.Unknown, nil
}
func (t *Transmitter) TaskFieldDimensions(target string, shardIDs []uint64, measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	return nil, nil, nil
}
func (t *Transmitter) TaskCreateIterator(target string, shardIDs []uint64, measurement *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
	return nil, nil
}

func (t *Transmitter) TaskMeasurementNames(target string, auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error) {
	//return []string{"192.168.0.1", "192.168.0.2", "192.168.0.3"}, nil
	m1 := "tab1"
	m2 := "tab2"
	m3 := "tab3"
	//m4 := "tab4"
	//m5 := "tab5"
	//m6 := "tab6"
	//m7 := "tab7"
	switch target {
	case "192.168.0.1":
		rst := make([][]byte, 0)
		rst = append(rst, []byte(m1))
		return rst, nil
	case "192.168.0.2":
		rst := make([][]byte, 0)
		rst = append(rst, []byte(m2))
		return nil, nil
	case "192.168.0.3":
		rst := make([][]byte, 0)
		rst = append(rst, []byte(m3))
		rst = append(rst, []byte(m1))
		return rst, nil
	}
	return nil, nil
}

func (t *Transmitter) TaskDeleteSeries(target string, database string, sources influxql.Sources, expr influxql.Expr) error {
	return nil
}
func (t *Transmitter) TaskDeleteMeasurement(target, database, name string) error {
	return nil
}
func (t *Transmitter) TaskDeleteRetentionPolicy(target, database, name string) error {
	return nil
}
func (t *Transmitter) TaskDeleteShard(target string, shardID uint64) error {
	return nil
}
func (t *Transmitter) TaskDeleteDatabase(target string, database string) error {
	return nil
}

func (t *Transmitter) TaskIteratorCost(target string, shardIDs []uint64, measurement string, opt query.IteratorOptions) (query.IteratorCost, error) {
	return query.IteratorCost{}, nil
}

//result map[measurement][]key
func (t *Transmitter) TaskTagKeys(target string, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) (map[string][]string, error) {
	rst := make(map[string][]string)
	switch shardIDs[0] {
	case 1:
		rst["key2"] = []string{"value1"}
	case 2:
		rst["key2"] = []string{"value2"}
	case 3:
		rst["key3"] = []string{"value3"}
	}
	return rst, nil
}

//result map[measurement]map[key][]value
func (t *Transmitter) TaskTagValues(target string, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) (map[string]map[string][]string, error) {
	rst := make(map[string]map[string][]string)
	switch shardIDs[0] {
	case 1:
		rst["m1"] = make(map[string][]string)
		rst["m1"]["key1"] = []string{"value1"}
	case 2:
		rst["m1"] = make(map[string][]string)
		rst["m1"]["key1"] = []string{"value2"}
	case 3:
		rst["m3"] = make(map[string][]string)
		rst["m3"]["key3"] = []string{"value3"}
	}
	return rst, nil
}

type MetaClient struct {
}

func (m *MetaClient) CreateContinuousQuery(database, name, query string) error {
	return nil
}
func (m *MetaClient) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	return nil, nil
}
func (m *MetaClient) CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
	return nil, nil
}
func (m *MetaClient) CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error) {
	return nil, nil
}
func (m *MetaClient) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return nil
}
func (m *MetaClient) CreateUser(name, password string, admin bool) (meta.User, error) {
	return nil, nil
}
func (m *MetaClient) Database(name string) *meta.DatabaseInfo {
	return nil
}
func (m *MetaClient) Databases() []meta.DatabaseInfo {
	return nil
}
func (m *MetaClient) DropShard(id uint64) error {
	return nil
}
func (m *MetaClient) DropContinuousQuery(database, name string) error {
	return nil
}
func (m *MetaClient) DropDatabase(name string) error {
	return nil
}
func (m *MetaClient) DropRetentionPolicy(database, name string) error {
	return nil
}
func (m *MetaClient) DropSubscription(database, rp, name string) error {
	return nil
}
func (m *MetaClient) DropUser(name string) error {
	return nil
}
func (m *MetaClient) RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error) {
	return nil, nil
}
func (m *MetaClient) SetAdminPrivilege(username string, admin bool) error {
	return nil
}
func (m *MetaClient) SetPrivilege(username, database string, p influxql.Privilege) error {
	return nil
}
func (m *MetaClient) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error) {
	return nil, nil
}
func (m *MetaClient) TruncateShardGroups(t time.Time) error {
	return nil
}
func (m *MetaClient) UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error {
	return nil
}
func (m *MetaClient) UpdateUser(name, password string) error {
	return nil
}
func (m *MetaClient) UserPrivilege(username, database string) (*influxql.Privilege, error) {
	return nil, nil
}
func (m *MetaClient) UserPrivileges(username string) (map[string]influxql.Privilege, error) {
	return nil, nil
}
func (m *MetaClient) Users() []meta.UserInfo {
	return nil
}

//Add by shan.jt provide the  necessary parameters
func (m *MetaClient) GetRPCHostByShardID(shardID uint64) (string, error) {
	switch shardID {
	case 1:
		return "192.168.0.1", nil
	case 2:
		return "192.168.0.2", nil
	case 3:
		return "192.168.0.3", nil
	}
	return "", nil
}

//Add by shan.jt provide the  necessary parameters
func (m *MetaClient) GetRPCHostsByShardID(shardID uint64) ([]string, error) {
	switch shardID {
	case 1:
		return []string{"192.168.0.1"}, nil
	case 2:
		return []string{"192.168.0.2"}, nil
	case 3:
		return []string{"192.168.0.3"}, nil
	}
	return nil, nil
}
func (m *MetaClient) GetRPCHostByNodeID(nodeID uint64) (string, error) {
	return "", nil
}
func (m *MetaClient) GetAllRPCHosts() ([]string, error) {
	return []string{"192.168.0.1", "192.168.0.2", "192.168.0.3"}, nil
}

//TaskIteratorCost(target string,shardIDs []uint64,measurement string,opt query.IteratorOptions)(query.IteratorCost, error)

//以下测试仅测试数据处理的正确性
func TestNewCluster(t *testing.T) {
	c := coordinator.NewCluster()
	c.Transmitter = &Transmitter{}
	c.MetaClient = &MetaClient{}
}
func TestCluster_TagKeys(t *testing.T) {
	c := coordinator.NewCluster()
	c.Transmitter = &Transmitter{}
	c.MetaClient = &MetaClient{}
	//cond := influxql.ParseExpr( )
	ids := []uint64{1, 2, 3}
	rst, err := c.TagKeys(query.OpenAuthorizer, ids, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(rst)
}
func TestCluster_TagValues(t *testing.T) {
	c := coordinator.NewCluster()
	c.Transmitter = &Transmitter{}
	c.MetaClient = &MetaClient{}
	ids := []uint64{1, 2, 3}
	rst, err := c.TagValues(query.OpenAuthorizer, ids, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(rst)
}
func TestCluster_DeleteMeasurement(t *testing.T) {
	c := coordinator.NewCluster()
	c.Transmitter = &Transmitter{}
	c.MetaClient = &MetaClient{}
	//cond := influxql.ParseExpr( )
	//ids := []uint64{1,2,3}
	err := c.DeleteMeasurement("database", "name")
	if err != nil {
		fmt.Println(err)
		return
	}
	//fmt.Println(rst)
}
func TestCluster_MeasurementNames(t *testing.T) {
	c := coordinator.NewCluster()
	c.Transmitter = &Transmitter{}
	c.MetaClient = &MetaClient{}
	names, err := c.MeasurementNames(query.OpenAuthorizer, "database", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, name := range names {
		fmt.Println(string(name))
	}
}
func TestCluster_DeleteDatabase(t *testing.T) {
	c := make(chan []byte, 5)
	c <- nil
	tmp := <-c
	fmt.Println(len(tmp))
}

func function(index int) (interface{}, error) {
	if index == 5 {
		return struct{}{}, errors.New("")
	}
	return struct{}{}, nil
}

func TestError(t *testing.T) {
	l := make([]interface{}, 10)
	errC := make(chan error, 10)
	var wg sync.WaitGroup
	for i, _ := range l {
		wg.Add(1)
		//如何在主线程中获取go func的返回值，并中断返回
		go func(index int) (err error) {
			defer func() {

				wg.Done()
			}()
			_, err = function(index)
			if err != nil {
				return
			}
			return
		}(i)
	}
	wg.Wait()
	fmt.Println("len errc is ", len(errC))
	for i := 0; i < len(l); i++ {
		if err := <-errC; err != nil {
			fmt.Println("error")
			return
		}
	}
	fmt.Println("no error")
}

func TestNewCluster2(t *testing.T) {
	defer func() {
		fmt.Println(1)
		fmt.Println(2)
	}()
	err := testnil()
	if err == nil {
		fmt.Println("err是空")
	} else {
		fmt.Println(err)
	}
}

func testnil() (err error) {
	return
}
