package meta

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	internal "github.com/influxdata/influxdb/services/meta/internal"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"go.uber.org/zap"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type Store struct {
	RaftDir     string
	LocalID     string
	RaftAddress string

	servers []raft.Server

	mu     sync.RWMutex
	data   *Data
	raft   *raft.Raft
	logger *zap.Logger
	cfg    *RaftConfig

	dataChanged chan struct{}

	Transmitter interface {
		JoinToCluster(target, raftID, raftAddr string) error
		ApplyToLeader(target string, body []byte) error
	}
}

func NewStore(c *RaftConfig) *Store {
	return &Store{
		LocalID:     c.LocalID,
		RaftDir:     c.RaftDir,
		RaftAddress: c.RaftAddress,

		data:        &Data{},
		mu:          sync.RWMutex{},
		dataChanged: make(chan struct{}),
		cfg:         c,
	}
}

func (s *Store) Open() error {
	//store 开启之前，应该先判定该节点是否要加入已存在的集群中
	if s.cfg.JoinAddress != "" {
		if err := s.Transmitter.JoinToCluster(s.cfg.JoinAddress, s.LocalID, s.RaftAddress); err != nil {
			return err
		}
	}

	if err := s.InitParam(); err != nil {
		return errors.New(fmt.Sprintf("[error] initialization raft param"))
	}
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.LocalID)
	config.ProtocolVersion = 3

	//Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftAddress)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftAddress, addr, 3, raftTimeout, os.Stderr)
	if err != nil {
		return err
	}
	//
	snapshot, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	//
	boltDb, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	ra, err := raft.NewRaft(config, (*FSMStore)(s), boltDb, boltDb, snapshot, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	//如果existing 状态为false 有两种情况，1：该集群首次启动 2：集群已启动，该节点首次启动
	//如果是情况1，则addaddress为空，只需要在本地执行启动就可以
	//如果是情况2，则需要将addaddress加入
	//集群仅限首次启动时启动cluster，如果bootcluster出错，其实代表正常
	if !s.cfg.Existing {
		configuration := raft.Configuration{
			Servers: s.servers,
		}
		future := ra.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			if err == raft.ErrCantBootstrap {
				return nil
			}
			return err
		}
	}
	return nil
}

func (s *Store) InitParam() error {
	ss, err := parseServers(s.cfg.Servers)
	if err != nil {
		return err
	}
	s.servers = ss
	s.LocalID = s.cfg.LocalID
	s.RaftAddress = s.cfg.RaftAddress
	s.RaftDir = s.cfg.RaftDir
	return nil
}

//在正确的前提下，解析server格式
func parseServers(ss []string) ([]raft.Server, error) {
	servers := make([]raft.Server, 0)
	for _, s := range ss {
		sl := strings.Split(s, "=")
		if len(sl) != 2 {
			return nil, errors.New("error server format")
		}
		if _, err := strconv.ParseInt(sl[0], 10, 64); err != nil {
			log.Fatal(err)
			return nil, errors.New("error server format")
		}
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(sl[0]),
			Address: raft.ServerAddress(sl[1]),
		})
	}
	return servers, nil
}

func (s *Store) Changed() {
	select {
	case <-s.dataChanged:
		return
	}
}

func (s *Store) GetData() *Data {
	s.mu.RLock()
	defer s.mu.RUnlock()
	other := s.data.Clone()
	return other
}

func (s *Store) CreateDatabase(name string) error {
	cmd := &internal.CreateDatabaseCommand{
		Name: proto.String(name),
	}
	if err := s.applyCommand(internal.Command_CreateDatabaseCommand, internal.E_CreateDatabaseCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}

func (s *Store) CreateRetentionPolicy(name string, rpi *RetentionPolicyInfo, true bool) error {
	cmd := &internal.CreateRetentionPolicyCommand{
		Database:        proto.String(name),
		RetentionPolicy: rpi.marshal(),
		MakeDefault:     proto.Bool(true),
	}
	if err := s.applyCommand(internal.Command_CreateRetentionPolicyCommand, internal.E_CreateRetentionPolicyCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) DropDatabase(name string) error {
	cmd := &internal.DropDatabaseCommand{
		Name: proto.String(name),
	}
	if err := s.applyCommand(internal.Command_DropDatabaseCommand, internal.E_DropDatabaseCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) DropRetentionPolicy(database, name string) error {
	cmd := &internal.DropRetentionPolicyCommand{
		Database: proto.String(database),
		Name:     &name,
	}
	if err := s.applyCommand(internal.Command_DropRetentionPolicyCommand, internal.E_DropRetentionPolicyCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) CreateUser(name, hash string, admin bool) error {
	cmd := &internal.CreateUserCommand{
		Name:  proto.String(name),
		Hash:  proto.String(hash),
		Admin: proto.Bool(admin),
	}
	if err := s.applyCommand(internal.Command_CreateUserCommand, internal.E_CreateUserCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) UpdateUser(name, hash string) error {
	cmd := &internal.UpdateUserCommand{
		Name: proto.String(name),
		Hash: proto.String(hash),
	}
	if err := s.applyCommand(internal.Command_UpdateUserCommand, internal.E_UpdateUserCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) DropUser(name string) error {
	cmd := &internal.DropUserCommand{
		Name: proto.String(name),
	}
	if err := s.applyCommand(internal.Command_DropUserCommand, internal.E_DropUserCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) SetPrivilege(username, database string, p influxql.Privilege) error {
	cmd := &internal.SetPrivilegeCommand{
		Username:  proto.String(username),
		Database:  proto.String(database),
		Privilege: proto.Int32(int32(p)),
	}
	if err := s.applyCommand(internal.Command_SetPrivilegeCommand, internal.E_SetPrivilegeCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) SetAdminPrivilege(username string, admin bool) error {
	cmd := &internal.SetAdminPrivilegeCommand{
		Username: proto.String(username),
		Admin:    proto.Bool(admin),
	}
	if err := s.applyCommand(internal.Command_SetAdminPrivilegeCommand, internal.E_SetAdminPrivilegeCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) DropShard(id uint64) error {
	cmd := &internal.DropShardCommand{
		ID: proto.Uint64(id),
	}
	if err := s.applyCommand(internal.Command_DropShardCommand, internal.E_DropShardCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) CreateShardGroup(database, policy string, timestamp time.Time) error {
	cmd := &internal.CreateShardGroupCommand{
		Database:  proto.String(database),
		Policy:    proto.String(policy),
		Timestamp: proto.Int64(timestamp.UnixNano()),
	}
	if err := s.applyCommand(internal.Command_CreateShardGroupCommand, internal.E_CreateShardGroupCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) DeleteShardGroup(database, policy string, id uint64) error {
	cmd := &internal.DeleteShardGroupCommand{
		Database:     proto.String(database),
		Policy:       proto.String(policy),
		ShardGroupID: proto.Uint64(id),
	}
	if err := s.applyCommand(internal.Command_DeleteShardGroupCommand, internal.E_DeleteShardGroupCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}

func (s *Store) CreateContinuousQuery(database, name, query string) error {
	cmd := &internal.CreateContinuousQueryCommand{
		Database: proto.String(database),
		Name:     proto.String(name),
		Query:    proto.String(query),
	}
	if err := s.applyCommand(internal.Command_CreateContinuousQueryCommand, internal.E_CreateContinuousQueryCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) DropContinuousQuery(database, name string) error {
	cmd := &internal.DropContinuousQueryCommand{
		Database: proto.String(database),
		Name:     proto.String(name),
	}
	if err := s.applyCommand(internal.Command_DropContinuousQueryCommand, internal.E_DropContinuousQueryCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) CreateSubscription(database, rp, name, mod string, destinations []string) error {
	cmd := &internal.CreateSubscriptionCommand{
		Database:        proto.String(database),
		RetentionPolicy: proto.String(rp),
		Name:            proto.String(name),
		Mode:            proto.String(mod),
		Destinations:    destinations,
	}
	if err := s.applyCommand(internal.Command_CreateSubscriptionCommand, internal.E_CreateSubscriptionCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) DropSubscription(database, rp, name string) error {
	cmd := &internal.DropSubscriptionCommand{
		Database:        proto.String(database),
		RetentionPolicy: proto.String(rp),
		Name:            proto.String(name),
	}
	if err := s.applyCommand(internal.Command_DropSubscriptionCommand, internal.E_DropSubscriptionCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}

func (s *Store) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate, makeDefault bool) error {
	cmd := &internal.UpdateRetentionPolicyCommand{
		Database:           proto.String(database),
		Name:               proto.String(name),
		NewName:            proto.String(*rpu.Name),
		Duration:           proto.Int64(int64(*rpu.Duration)),
		ReplicaN:           proto.Uint32(uint32(*rpu.ReplicaN)),
		ShardGroupDuration: proto.Int64(int64(*rpu.ShardGroupDuration)),
		MakeDefault:        proto.Bool(makeDefault),
	}
	if err := s.applyCommand(internal.Command_UpdateRetentionPolicyCommand, internal.E_UpdateRetentionPolicyCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) UpdateNodeInfo(info *NodeInfo) error {
	cmd := &internal.UpdateDataNodeCommand{
		ID:      &info.ID,
		Host:    &info.Host,
		TCPHost: &info.TCPHost,
	}
	if err := s.applyCommand(internal.Command_UpdateDataNodeCommand, internal.E_UpdateDataNodeCommand_Command, cmd); err != nil {
		return err
	}
	return nil
}
func (s *Store) applyCommand(command_type internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	cmd := &internal.Command{Type: &command_type}
	if err := proto.SetExtension(cmd, desc, value); err != nil {
		panic(err)
	}
	b, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	//若本身不是leader，找到leader的地址，自动步进到leader提交
	if s.raft.Leader() != raft.ServerAddress(s.RaftAddress) {
		leaderRPC, err := s.GetLeaderRPCHost()
		if err != nil {
			return err
		}
		if err := s.Transmitter.ApplyToLeader(leaderRPC, b); err != nil {
			return err
		}
		return nil
	}
	//否则本地执行
	if err := s.LocalApply(b); err != nil {
		return err
	}
	return nil
}
func (s *Store) GetLeaderRPCHost() (string, error) {
	leader := s.raft.Leader()
	if leader == "" {
		return "", fmt.Errorf("[error] get raft leader is nil")
	}
	s.mu.RLock()
	other := s.data.Clone()
	s.mu.RUnlock()
	for _, ni := range other.DataNodes {
		if ni.Host == string(leader) {
			return ni.TCPHost, nil
		}
	}
	return "", fmt.Errorf("[error] can't find leader's rpc host in DataNodes")
}
func (s *Store) LocalApply(b []byte) error {
	future := s.raft.Apply(b, raftTimeout)
	if err := future.Error(); err != nil {
		return err
	}
	return nil
}
func (s *Store) LocalJoin(raftID, raftAddr string) error {
	future := s.raft.AddVoter(raft.ServerID(raftID), raft.ServerAddress(raftAddr), 0, raftTimeout)
	if err := future.Error(); err != nil {
		return err
	}
	return nil
}
