// Package meta provides control over meta data for InfluxDB,
// such as controlling databases, retention policies, users, etc.
package meta

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"context"
	"fmt"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/file"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

const (
	// SaltBytes is the number of bytes used for salts.
	SaltBytes = 32

	metaFile = "meta.db"

	// ShardGroupDeletedExpiration is the amount of time before a shard group info will be removed from cached
	// data after it has been marked deleted (2 weeks).
	ShardGroupDeletedExpiration = -2 * 7 * 24 * time.Hour

	DefaultCacheDataDir = "/influxdb/cachedata"

	DefaultProxyNodeDir = "/influxdb/agent"

	DefaultProxyPeersHost = ":1999"
)

var (
	// ErrServiceUnavailable is returned when the meta service is unavailable.
	ErrServiceUnavailable = errors.New("meta service unavailable")

	// ErrService is returned when the meta service returns an error.
	ErrService = errors.New("meta service error")

	//
	ErrCanFindNodeByNodeID = errors.New("can't find node address with nodeID")

	ErrCanFindNodeByShardID = errors.New("can't find node address with shardID")

	ErrCanFindNodeIDByShardID = errors.New("can't find nodeID with shardID")
)

type RaftStore interface {
	Open() error
	Changed()
	GetData() *Data

	CreateDatabase(name string) error
	CreateRetentionPolicy(name string, rpi *RetentionPolicyInfo, makeDefault bool) error
	UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate, makeDefault bool) error
	//当一个节点上线，id是他唯一的标识符，如果raft中有相同的id，则更新信息，若没有，则添加信息。
	UpdateNodeInfo(info *NodeInfo) error
	DropDatabase(name string) error
	DropRetentionPolicy(database, name string) error
	CreateUser(name, hash string, admin bool) error
	UpdateUser(name, hash string) error
	DropUser(name string) error
	SetPrivilege(name, database string, p influxql.Privilege) error
	SetAdminPrivilege(name string, admin bool) error
	DropShard(id uint64) error
	CreateShardGroup(database, policy string, timestamp time.Time) error
	DeleteShardGroup(database, policy string, id uint64) error
	CreateContinuousQuery(database, name, query string) error
	DropContinuousQuery(database, name string) error
	CreateSubscription(database, rp, name, mode string, destinations []string) error
	DropSubscription(database, rp, name string) error
}

// Client is used to execute commands on and read data from
// a meta service cluster.
type Client struct {
	logger *zap.Logger

	mu        sync.RWMutex
	closing   chan struct{}
	changed   chan struct{}
	cacheData *Data

	Store     RaftStore
	rconfig   *RaftConfig
	authCache map[string]authUser

	path string

	retentionAutoCreate bool

	ctx  context.Context
	Node *influxdb.Node
}

type authUser struct {
	bhash string
	salt  []byte
	hash  []byte
}

// NewClient returns a new *Client.
func NewClient(config *Config) *Client {
	//s := NewStore(rconfig)
	return &Client{
		cacheData:           &Data{},
		closing:             make(chan struct{}),
		changed:             make(chan struct{}),
		logger:              zap.NewNop(),
		authCache:           make(map[string]authUser),
		path:                config.Dir,
		retentionAutoCreate: config.RetentionAutoCreate,

		//TODO(shan.jt for cluster)
		ctx: context.Background(),
		//Store:s,
		//rconfig:rconfig,
	}
}

// Open a connection to a meta service cluster.
func (c *Client) Open() error {

	//出于延迟考虑，万一后同步会导致chan没有收到关闭信号而错过部分数据
	//第1步，开始同步数据
	go c.UpdateCacheData()

	//第2步，开启raft node
	if err := c.Store.Open(); err != nil {
		return fmt.Errorf("open raft store %s", err)
	}

	//第3步，更新本节点信息
	node := &NodeInfo{
		ID:      c.Node.ID,
		Host:    c.Node.Host,
		TCPHost: c.Node.RpcAddress,
	}
	//延迟更新节点且务必成功，因为首节点启动时没有leader，所以只有当raft集群完成后才能正常写入本节点信息
	go func() {
		for true {
			if err := c.Store.UpdateNodeInfo(node); err != nil {
				c.logger.Warn("update local node info %s", zap.String("error", err.Error()))
				time.Sleep(2 * time.Second)
				continue
			}
			break
		}
	}()
	return nil
}

//检测到store有变化，就更新meta缓存
//changed 本身是一个阻塞函数，只有变换时才返回
func (c *Client) UpdateCacheData() {
	for {
		c.Store.Changed()
		d := c.Store.GetData()
		c.mu.Lock()
		c.cacheData = d
		c.mu.Unlock()
	}
}

// Close the meta service cluster connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}

	select {
	case <-c.closing:
		return nil
	default:
		close(c.closing)
	}

	return nil
}

// AcquireLease attempts to acquire the specified lease.
// TODO corylanou remove this for single node
func (c *Client) AcquireLease(name string) (*Lease, error) {
	l := Lease{
		Name:       name,
		Expiration: time.Now().Add(DefaultLeaseDuration),
	}
	return &l, nil
}

// ClusterID returns the ID of the cluster it's connected to.
func (c *Client) ClusterID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cacheData.ClusterID
}

// Database returns info for the requested database.
func (c *Client) Database(name string) *DatabaseInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, d := range c.cacheData.Databases {
		if d.Name == name {
			return &d
		}
	}

	return nil
}

// Databases returns a list of all database infos.
func (c *Client) Databases() []DatabaseInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dbs := c.cacheData.Databases
	if dbs == nil {
		return []DatabaseInfo{}
	}
	return dbs
}

// CreateDatabase creates a database or returns it if it already exists.
func (c *Client) CreateDatabase(name string) (*DatabaseInfo, error) {
	c.mu.RLock()
	data := c.cacheData.Clone()
	c.mu.RUnlock()
	if db := data.Database(name); db != nil {
		return db, nil
	}
	//使用raft方式进行修改
	if err := c.Store.CreateDatabase(name); err != nil {
		return nil, err
	}
	if c.retentionAutoCreate {
		rpi := DefaultRetentionPolicyInfo()
		if err := c.Store.CreateRetentionPolicy(name, rpi, true); err != nil {
			return nil, err
		}
	}
	db := c.Database(name)
	return db, nil
}

// CreateDatabaseWithRetentionPolicy creates a database with the specified
// retention policy.
//
// When creating a database with a retention policy, the retention policy will
// always be set to default. Therefore if the caller provides a retention policy
// that already exists on the database, but that retention policy is not the
// default one, an error will be returned.
//
// This call is only idempotent when the caller provides the exact same
// retention policy, and that retention policy is already the default for the
// database.
//
func (c *Client) CreateDatabaseWithRetentionPolicy(name string, spec *RetentionPolicySpec) (*DatabaseInfo, error) {
	if spec == nil {
		return nil, errors.New("CreateDatabaseWithRetentionPolicy called with nil spec")
	}
	c.mu.RLock()
	data := c.cacheData.Clone()
	c.mu.RUnlock()

	if spec.Duration != nil && *spec.Duration < MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	db := data.Database(name)
	if db == nil {
		if err := c.Store.CreateDatabase(name); err != nil {
			return nil, err
		}
	}

	db = c.Database(name)
	// No existing retention policies, so we can create the provided policy as
	// the new default policy.
	rpi := spec.NewRetentionPolicyInfo()
	if len(db.RetentionPolicies) == 0 {
		if err := c.Store.CreateRetentionPolicy(name, rpi, true); err != nil {
			return nil, err
		}
	} else if !spec.Matches(db.RetentionPolicy(rpi.Name)) {
		// In this case we already have a retention policy on the database and
		// the provided retention policy does not match it. Therefore, this call
		// is not idempotent and we need to return an error.
		return nil, ErrRetentionPolicyConflict
	}

	// If a non-default retention policy was passed in that already exists then
	// it's an error regardless of if the exact same retention policy is
	// provided. CREATE DATABASE WITH RETENTION POLICY should only be used to
	// create DEFAULT retention policies.
	if db.DefaultRetentionPolicy != rpi.Name {
		return nil, ErrRetentionPolicyConflict
	}

	// Commit the changes.
	//if err := c.commit(data); err != nil {
	//	return nil, err
	//}

	// Refresh the database info.
	db = c.Database(name)
	return db, nil
}

// DropDatabase deletes a database.
func (c *Client) DropDatabase(name string) error {
	if err := c.Store.DropDatabase(name); err != nil {
		return err
	}
	return nil
}

// CreateRetentionPolicy creates a retention policy on the specified database.
func (c *Client) CreateRetentionPolicy(database string, spec *RetentionPolicySpec, makeDefault bool) (*RetentionPolicyInfo, error) {
	if spec.Duration != nil && *spec.Duration < MinRetentionPolicyDuration && *spec.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	rp := spec.NewRetentionPolicyInfo()
	if err := c.Store.CreateRetentionPolicy(database, rp, makeDefault); err != nil {
		return nil, err
	}
	return rp, nil
}

// RetentionPolicy returns the requested retention policy info.
func (c *Client) RetentionPolicy(database, name string) (rpi *RetentionPolicyInfo, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db := c.cacheData.Database(database)
	if db == nil {
		return nil, influxdb.ErrDatabaseNotFound(database)
	}

	return db.RetentionPolicy(name), nil
}

// DropRetentionPolicy drops a retention policy from a database.
func (c *Client) DropRetentionPolicy(database, name string) error {
	if err := c.Store.DropRetentionPolicy(database, name); err != nil {
		return err
	}
	return nil
}

// UpdateRetentionPolicy updates a retention policy.
func (c *Client) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate, makeDefault bool) error {
	//c.mu.Lock()
	//defer c.mu.Unlock()

	//data := c.cacheData.Clone()

	if err := c.Store.UpdateRetentionPolicy(database, name, rpu, makeDefault); err != nil {
		return err
	}

	return nil
}

// Users returns a slice of UserInfo representing the currently known users.
func (c *Client) Users() []UserInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	users := c.cacheData.Users

	if users == nil {
		return []UserInfo{}
	}
	return users
}

// User returns the user with the given name, or ErrUserNotFound.
func (c *Client) User(name string) (User, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, u := range c.cacheData.Users {
		if u.Name == name {
			return &u, nil
		}
	}

	return nil, ErrUserNotFound
}

// bcryptCost is the cost associated with generating password with bcrypt.
// This setting is lowered during testing to improve test suite performance.
var bcryptCost = bcrypt.DefaultCost

// hashWithSalt returns a salted hash of password using salt.
func (c *Client) hashWithSalt(salt []byte, password string) []byte {
	hasher := sha256.New()
	hasher.Write(salt)
	hasher.Write([]byte(password))
	return hasher.Sum(nil)
}

// saltedHash returns a salt and salted hash of password.
func (c *Client) saltedHash(password string) (salt, hash []byte, err error) {
	salt = make([]byte, SaltBytes)
	if _, err := io.ReadFull(crand.Reader, salt); err != nil {
		return nil, nil, err
	}

	return salt, c.hashWithSalt(salt, password), nil
}

// CreateUser adds a user with the given name and password and admin status.
func (c *Client) CreateUser(name, password string, admin bool) (User, error) {
	c.mu.RLock()
	data := c.cacheData.Clone()
	c.mu.RUnlock()
	// See if the user already exists.
	if u := data.user(name); u != nil {
		if err := bcrypt.CompareHashAndPassword([]byte(u.Hash), []byte(password)); err != nil || u.Admin != admin {
			return nil, ErrUserExists
		}
		return u, nil
	}

	// Hash the password before serializing it.
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return nil, err
	}

	if err := c.Store.CreateUser(name, string(hash), admin); err != nil {
		return nil, err
	}
	u, err := c.User(name)
	return u, nil
}

// UpdateUser updates the password of an existing user.
func (c *Client) UpdateUser(name, password string) error {
	// Hash the password before serializing it.
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return err
	}
	if err := c.Store.UpdateUser(name, string(hash)); err != nil {
		return err
	}
	delete(c.authCache, name)

	return nil
}

// DropUser removes the user with the given name.
func (c *Client) DropUser(name string) error {
	if err := c.Store.DropUser(name); err != nil {
		return err
	}
	return nil
}

// SetPrivilege sets a privilege for the given user on the given database.
func (c *Client) SetPrivilege(username, database string, p influxql.Privilege) error {
	//c.mu.Lock()
	//defer c.mu.Unlock()

	if err := c.Store.SetPrivilege(username, database, p); err != nil {
		return err
	}

	return nil
}

// SetAdminPrivilege sets or unsets admin privilege to the given username.
func (c *Client) SetAdminPrivilege(username string, admin bool) error {
	if err := c.Store.SetAdminPrivilege(username, admin); err != nil {
		return err
	}
	return nil
}

// UserPrivileges returns the privileges for a user mapped by database name.
func (c *Client) UserPrivileges(username string) (map[string]influxql.Privilege, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	p, err := c.cacheData.UserPrivileges(username)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// UserPrivilege returns the privilege for the given user on the given database.
func (c *Client) UserPrivilege(username, database string) (*influxql.Privilege, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	p, err := c.cacheData.UserPrivilege(username, database)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// AdminUserExists returns true if any user has admin privilege.
func (c *Client) AdminUserExists() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.AdminUserExists()
}

// Authenticate returns a UserInfo if the username and password match an existing entry.
func (c *Client) Authenticate(username, password string) (User, error) {
	// Find user.
	c.mu.RLock()
	userInfo := c.cacheData.user(username)
	c.mu.RUnlock()
	if userInfo == nil {
		return nil, ErrUserNotFound
	}

	// Check the local auth cache first.
	c.mu.RLock()
	au, ok := c.authCache[username]
	c.mu.RUnlock()
	if ok {
		// verify the password using the cached salt and hash
		if bytes.Equal(c.hashWithSalt(au.salt, password), au.hash) {
			return userInfo, nil
		}

		// fall through to requiring a full bcrypt hash for invalid passwords
	}

	// Compare password with user hash.
	if err := bcrypt.CompareHashAndPassword([]byte(userInfo.Hash), []byte(password)); err != nil {
		return nil, ErrAuthenticate
	}

	// generate a salt and hash of the password for the cache
	salt, hashed, err := c.saltedHash(password)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.authCache[username] = authUser{salt: salt, hash: hashed, bhash: userInfo.Hash}
	c.mu.Unlock()
	return userInfo, nil
}

// UserCount returns the number of users stored.
func (c *Client) UserCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.cacheData.Users)
}

// ShardIDs returns a list of all shard ids.
func (c *Client) ShardIDs() []uint64 {
	c.mu.RLock()

	var a []uint64
	for _, dbi := range c.cacheData.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				for _, si := range sgi.Shards {
					a = append(a, si.ID)
				}
			}
		}
	}
	c.mu.RUnlock()
	sort.Sort(uint64Slice(a))
	return a
}

// ShardGroupsByTimeRange returns a list of all shard groups on a database and policy that may contain data
// for the specified time range. Shard groups are sorted by start time.
func (c *Client) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []ShardGroupInfo, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Find retention policy.
	rpi, err := c.cacheData.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, influxdb.ErrRetentionPolicyNotFound(policy)
	}
	groups := make([]ShardGroupInfo, 0, len(rpi.ShardGroups))
	for _, g := range rpi.ShardGroups {
		if g.Deleted() || !g.Overlaps(min, max) {
			continue
		}
		groups = append(groups, g)
	}
	return groups, nil
}

// ShardsByTimeRange returns a slice of shards that may contain data in the time range.
func (c *Client) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []ShardInfo, err error) {
	m := make(map[*ShardInfo]struct{})
	for _, mm := range sources.Measurements() {
		groups, err := c.ShardGroupsByTimeRange(mm.Database, mm.RetentionPolicy, tmin, tmax)
		if err != nil {
			return nil, err
		}
		for _, g := range groups {
			for i := range g.Shards {
				m[&g.Shards[i]] = struct{}{}
			}
		}
	}

	a = make([]ShardInfo, 0, len(m))
	for sh := range m {
		a = append(a, *sh)
	}

	return a, nil
}

// DropShard deletes a shard by ID.
func (c *Client) DropShard(id uint64) error {
	if err := c.Store.DropShard(id); err != nil {
		return err
	}
	return nil
}

//TODO(just don't how to use it)
// TruncateShardGroups truncates any shard group that could contain timestamps beyond t.
func (c *Client) TruncateShardGroups(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	data := c.cacheData.Clone()
	data.TruncateShardGroups(t)
	return nil
}

//TODO(just don't know how to use it)
// PruneShardGroups remove deleted shard groups from the data store.
func (c *Client) PruneShardGroups() error {
	var changed bool
	expiration := time.Now().Add(ShardGroupDeletedExpiration)
	c.mu.Lock()
	defer c.mu.Unlock()
	data := c.cacheData.Clone()
	for i, d := range data.Databases {
		for j, rp := range d.RetentionPolicies {
			var remainingShardGroups []ShardGroupInfo
			for _, sgi := range rp.ShardGroups {
				if sgi.DeletedAt.IsZero() || !expiration.After(sgi.DeletedAt) {
					remainingShardGroups = append(remainingShardGroups, sgi)
					continue
				}
				changed = true
			}
			data.Databases[i].RetentionPolicies[j].ShardGroups = remainingShardGroups
		}
	}
	if changed {
		return c.commit(data)
	}
	return nil
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
func (c *Client) CreateShardGroup(database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	// Check under a read-lock
	c.mu.RLock()
	if sg, _ := c.cacheData.ShardGroupByTimestamp(database, policy, timestamp); sg != nil {
		c.mu.RUnlock()
		return sg, nil
	}
	data := c.cacheData.Clone()
	c.mu.RUnlock()

	//c.mu.Lock()
	//defer c.mu.Unlock()

	// Check again under the write lock
	if sg, _ := data.ShardGroupByTimestamp(database, policy, timestamp); sg != nil {
		return sg, nil
	}
	sgi, err := createShardGroup(c, database, policy, timestamp)
	if err != nil {
		return nil, err
	}
	return sgi, nil
}

func createShardGroup(c *Client, database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	if err := c.Store.CreateShardGroup(database, policy, timestamp); err != nil {
		return nil, err
	}

	rpi, err := c.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, errors.New("retention policy deleted after shard group created")
	}
	sgi := rpi.ShardGroupByTimestamp(timestamp)
	return sgi, nil
}

// DeleteShardGroup removes a shard group from a database and retention policy by id.
func (c *Client) DeleteShardGroup(database, policy string, id uint64) error {
	if err := c.Store.DeleteShardGroup(database, policy, id); err != nil {
		return err
	}
	return nil
}

// PrecreateShardGroups creates shard groups whose endtime is before the 'to' time passed in, but
// is yet to expire before 'from'. This is to avoid the need for these shards to be created when data
// for the corresponding time range arrives. Shard creation involves Raft consensus, and precreation
// avoids taking the hit at write-time.
func (c *Client) PrecreateShardGroups(from, to time.Time) error {
	c.mu.Lock()
	data := c.cacheData.Clone()
	c.mu.Unlock()
	//var changed bool

	for _, di := range data.Databases {
		for _, rp := range di.RetentionPolicies {
			if len(rp.ShardGroups) == 0 {
				// No data was ever written to this group, or all groups have been deleted.
				continue
			}
			g := rp.ShardGroups[len(rp.ShardGroups)-1] // Get the last group in time.
			if !g.Deleted() && g.EndTime.Before(to) && g.EndTime.After(from) {
				// Group is not deleted, will end before the future time, but is still yet to expire.
				// This last check is important, so the system doesn't create shards groups wholly
				// in the past.

				// Create successive shard group.
				nextShardGroupTime := g.EndTime.Add(1 * time.Nanosecond)
				// if it already exists, continue
				if sg, _ := data.ShardGroupByTimestamp(di.Name, rp.Name, nextShardGroupTime); sg != nil {
					c.logger.Info("Shard group already exists",
						logger.ShardGroup(sg.ID),
						logger.Database(di.Name),
						logger.RetentionPolicy(rp.Name))
					continue
				}
				newGroup, err := createShardGroup(c, di.Name, rp.Name, nextShardGroupTime)
				if err != nil {
					c.logger.Info("Failed to precreate successive shard group",
						zap.Uint64("group_id", g.ID), zap.Error(err))
					continue
				}
				//changed = true
				c.logger.Info("New shard group successfully precreated",
					logger.ShardGroup(newGroup.ID),
					logger.Database(di.Name),
					logger.RetentionPolicy(rp.Name))
			}
		}
	}
	return nil
}

// ShardOwner returns the owning shard group info for a specific shard.
func (c *Client) ShardOwner(shardID uint64) (database, policy string, sgi *ShardGroupInfo) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, dbi := range c.cacheData.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, g := range rpi.ShardGroups {
				if g.Deleted() {
					continue
				}

				for _, sh := range g.Shards {
					if sh.ID == shardID {
						database = dbi.Name
						policy = rpi.Name
						sgi = &g
						return
					}
				}
			}
		}
	}
	return
}

// CreateContinuousQuery saves a continuous query with the given name for the given database.
func (c *Client) CreateContinuousQuery(database, name, query string) error {

	if err := c.Store.CreateContinuousQuery(database, name, query); err != nil {
		return err
	}
	return nil
}

// DropContinuousQuery removes the continuous query with the given name on the given database.
func (c *Client) DropContinuousQuery(database, name string) error {
	if err := c.Store.DropContinuousQuery(database, name); err != nil {
		return err
	}
	return nil
}

// CreateSubscription creates a subscription against the given database and retention policy.
func (c *Client) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	if err := c.Store.CreateSubscription(database, rp, name, mode, destinations); err != nil {
		return err
	}
	return nil
}

// DropSubscription removes the named subscription from the given database and retention policy.
func (c *Client) DropSubscription(database, rp, name string) error {
	if err := c.Store.DropSubscription(database, rp, name); err != nil {
		return err
	}
	return nil
}

//TODO(外部使用，暂时未实现)
// SetData overwrites the underlying data in the meta store.
func (c *Client) SetData(data *Data) error {
	c.mu.Lock()

	// reset the index so the commit will fire a change event
	//c.cacheData.Index = 0

	// increment the index to force the changed channel to fire
	d := data.Clone()
	//d.Index++

	if err := c.commit(d); err != nil {
		return err
	}

	c.mu.Unlock()

	return nil
}

// Data returns a clone of the underlying data in the meta store.
func (c *Client) Data() Data {
	c.mu.RLock()
	defer c.mu.RUnlock()
	d := c.cacheData.Clone()
	return *d
}

// WaitForDataChanged returns a channel that will get closed when
// the metastore data has changed.
func (c *Client) WaitForDataChanged() chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.changed
}

//TODO()该功能无用
// commit writes data to the underlying store.
// This method assumes c's mutex is already locked.
func (c *Client) commit(data *Data) error {
	//data.Index++

	//TODO(shan.jt for cluster) commit change data to remote

	//if err := c.Agent.PutCacheData(DefaultCacheDataDir, data); err != nil {
	//	return err
	//}

	// try to write to disk before updating in memory
	if err := snapshot(c.path, data); err != nil {
		return err
	}

	//TODO(shan.jt) we update memory with sync only --delete
	// update in memory
	//c.cacheData = data

	// close channels to signal changes
	close(c.changed)
	c.changed = make(chan struct{})

	return nil
}

// MarshalBinary returns a binary representation of the underlying data.
func (c *Client) MarshalBinary() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cacheData.MarshalBinary()
}

// WithLogger sets the logger for the client.
func (c *Client) WithLogger(log *zap.Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger = log.With(zap.String("service", "metaclient"))
}

// snapshot saves the current meta data to disk.
func snapshot(path string, data *Data) error {
	filename := filepath.Join(path, metaFile)
	tmpFile := filename + "tmp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer f.Close()

	var d []byte
	if b, err := data.MarshalBinary(); err != nil {
		return err
	} else {
		d = b
	}

	if _, err := f.Write(d); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return err
	}

	//close file handle before renaming to support Windows
	if err = f.Close(); err != nil {
		return err
	}

	return file.RenameFile(tmpFile, filename)
}

//CreateETcdClient is a function for test only
//func (c *Client) CreateEtcdClient(endpoints []string) error {
//	conf := clientv3.Config{
//		Endpoints: endpoints,
//	}
//	cli, err := clientv3.New(conf)
//	if nil != err {
//		return err
//	}
//	c.c = cli
//	return nil
//}

func (c *Client) NodeID() uint64 {
	return c.Node.ID
}

//根据nodeid获取node address,如果cache的datanode中不存在nodeid，则返回空字符串和err
//如果节点下线，则返回错误
func (c *Client) GetRPCHostByNodeID(nodeID uint64) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, node := range c.cacheData.DataNodes {
		if node.ID == nodeID {
			//if _, ok := c.NodeState[node.ID]; !ok || c.NodeState[node.ID] != Online {
			//return "", errors.New(fmt.Sprintf("node %d has offline", nodeID))
			//}
			return node.TCPHost, nil
		}
	}
	return "", errors.New(fmt.Sprintf("can't find node %d in DataNodes", nodeID))
}

//在datanode中获取所有的rpc地址
func (c *Client) GetAllRPCHosts() ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	hosts := make([]string, 0)
	for _, node := range c.cacheData.DataNodes {
		//if _, ok := c.NodeState[node.ID]; !ok || c.NodeState[node.ID] != Online {
		//return nil, errors.New(fmt.Sprintf("node %d has offline", node.ID))
		//}
		hosts = append(hosts, node.TCPHost)
	}
	if len(hosts) == 0 {
		return nil, errors.New("have empty nodeinfo in cachedata")
	}
	return hosts, nil
}

//根据shardID获取所有nodeID，若有节点离线，则返回错误
func (c *Client) GetRPCHostsByShardID(shardID uint64) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	nodeIDs, err := c.GetNodeIDsByShardID(shardID)
	if err != nil {
		return nil, err
	}
	hosts := make([]string, 0)
	for _, nodeID := range nodeIDs {
		host, err := c.GetRPCHostByNodeID(nodeID)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, host)
	}
	return hosts, nil
}

//TODO(shan.jt for cluster) we should update the cacheData from remote and add us node to cache data if it is first start in cluster
// Load loads the current meta data from disk.
//func (c *Client) Load() error {
//	data, err := c.Agent.GetCacheData(DefaultCacheDataDir)
//	if err != nil {
//		return err
//	}
//	isNewNode := true
//	for i, node := range data.DataNodes {
//		if node.ID == c.Node.ID {
//			data.DataNodes[i].RPCHost = c.Node.RpcAddress
//			data.DataNodes[i].Host = c.Node.Host
//			isNewNode = false
//		}
//	}
//	if isNewNode {
//		data.DataNodes = append(data.DataNodes, NodeInfo{
//			ID:      c.Node.ID,
//			RPCHost: c.Node.RpcAddress,
//			Host:    c.Node.Host,
//		})
//	}
//	if err := c.commit(data); err != nil {
//		return err
//	}
//	return nil
//}

//根据shardID获取address时，只获取获取在线的node
func (c *Client) GetRPCHostByShardID(shardID uint64) (string, error) {
	c.mu.RLock()
	c.mu.RUnlock()
	nodeIDs, err := c.GetNodeIDsByShardID(shardID)
	if err != nil {
		return "", err
	}
	host := ""
	for _, nodeID := range nodeIDs {
		host, err = c.GetRPCHostByNodeID(nodeID)
		if err != nil {
			continue
		}
	}
	if host == "" {
		return "", errors.New(fmt.Sprintf("shard %d don't have active node", shardID))
	}
	return host, nil
}

//返回在线的节点
//TODO()暂时取消节点上下线检测
func (c *Client) GetOnlineNodeByShardID(si ShardInfo) (uint64, error) {
	for _, ni := range si.Owners {
		//if _, ok := c.NodeState[ni.NodeID]; ok && c.NodeState[ni.NodeID] == Online {
		return ni.NodeID, nil
		//}
	}
	return 0, errors.New(fmt.Sprintf("shard %d don't have active node", si.ID))
}

//在shardinfo中获取该shard的所有owners
func (c *Client) GetNodeIDsByShardID(shardID uint64) ([]uint64, error) {
	c.mu.RLock()
	c.mu.RUnlock()
	nodeIDs := make([]uint64, 0)
	for _, dbi := range c.cacheData.Databases {
		for _, rpi := range dbi.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				for _, si := range sgi.Shards {
					if si.ID == shardID {
						for _, own := range si.Owners {
							nodeIDs = append(nodeIDs, own.NodeID)
						}
						goto FINDIT
					}
				}
			}
		}
	}
FINDIT:
	if len(nodeIDs) == 0 {
		return nil, errors.New(fmt.Sprintf("shard %d don't has owners", shardID))
	}
	return nodeIDs, nil
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }
