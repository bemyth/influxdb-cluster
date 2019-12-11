package coordinator

import (
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
)

// MetaClient is an interface for accessing meta data.
type MetaClient interface {
	CreateContinuousQuery(database, name, query string) error
	CreateDatabase(name string) (*meta.DatabaseInfo, error)
	CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
	CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error)
	CreateSubscription(database, rp, name, mode string, destinations []string) error
	CreateUser(name, password string, admin bool) (meta.User, error)
	Database(name string) *meta.DatabaseInfo
	Databases() []meta.DatabaseInfo
	DropShard(id uint64) error
	DropContinuousQuery(database, name string) error
	DropDatabase(name string) error
	DropRetentionPolicy(database, name string) error
	DropSubscription(database, rp, name string) error
	DropUser(name string) error
	RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error)
	SetAdminPrivilege(username string, admin bool) error
	SetPrivilege(username, database string, p influxql.Privilege) error
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	TruncateShardGroups(t time.Time) error
	UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
	UpdateUser(name, password string) error
	UserPrivilege(username, database string) (*influxql.Privilege, error)
	UserPrivileges(username string) (map[string]influxql.Privilege, error)
	Users() []meta.UserInfo

	//Add by shan.jt provide the  necessary parameters
	//获取shardID所有的节点地址，若有节点离线，返回错误
	GetRPCHostsByShardID(shardID uint64) ([]string, error)
	//获取shardID对应的节点地址，尽量选择在线节点，若全部节点都不在线，返回错误
	GetRPCHostByShardID(shardID uint64) (string, error)
	//获取指定节点的节点地址，若该节点离线，则返回错误
	GetRPCHostByNodeID(nodeID uint64) (string, error)
	//获取所有元数据中的节点地址，无论其是否在线，若有节点离线，则返回错误
	GetAllRPCHosts() ([]string, error)
}
