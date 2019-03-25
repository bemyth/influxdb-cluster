package meta

import (
	"time"
)

type Data struct {
	Term      uint64
	Index     uint64
	ClusterID uint64

	Databases []DatabaseInfo
	DataNode  []NodeInfo
	MetaNode  []NodeInfo

	//MaxMetaNodeID uint64
	//MaxDataNodeID uint64
}

type DatabaseInfo struct {
	Name                   string
	DefaultRetentionPolicy string
	RetentionPolicies      []RetentionPolicyInfo
	ContinuousQueries      []ContinuousQueryInfo
}

type NodeInfo struct {
	ID      uint64
	Host    string
	TCPHost string
}

// RetentionPolicyInfo represents metadata about a retention policy.
type RetentionPolicyInfo struct {
	Name               string
	ReplicaN           int
	Duration           time.Duration
	ShardGroupDuration time.Duration
	ShardGroups        []ShardGroupInfo
	Subscriptions      []SubscriptionInfo
}

// ShardGroupInfo represents metadata about a shard group. The DeletedAt field is important
// because it makes it clear that a ShardGroup has been marked as deleted, and allow the system
// to be sure that a ShardGroup is not simply missing. If the DeletedAt is set, the system can
// safely delete any associated shards.
type ShardGroupInfo struct {
	ID          uint64
	StartTime   time.Time
	EndTime     time.Time
	DeletedAt   time.Time
	Shards      []ShardInfo
	TruncatedAt time.Time
}

// SubscriptionInfo holds the subscription information.
type SubscriptionInfo struct {
	Name         string
	Mode         string
	Destinations []string
}

// ShardInfo represents metadata about a shard.
type ShardInfo struct {
	ID     uint64
	Owners []ShardOwner
}

// ShardOwner represents a node that owns a shard.
type ShardOwner struct {
	NodeID uint64
}

// ContinuousQueryInfo represents metadata about a continuous query.
type ContinuousQueryInfo struct {
	Name  string
	Query string
}

//TODO complete
func (data *Data) MarshBinary() ([]byte, error) {
	return nil, nil
}
