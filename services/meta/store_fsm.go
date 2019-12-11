package meta

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	internal "github.com/influxdata/influxdb/services/meta/internal"
	"github.com/influxdata/influxql"
	"io"
	"io/ioutil"
	"time"
)

type FSMStore Store

func (f *FSMStore) Apply(l *raft.Log) interface{} {
	var cmd internal.Command
	if err := proto.Unmarshal(l.Data, &cmd); err != nil {
		panic(fmt.Errorf("cannot marshal command: %x", l.Data))
	}
	//s := (*Store)(f)
	f.mu.Lock()
	defer f.mu.Unlock()
	err := func() interface{} {
		switch cmd.GetType() {
		case internal.Command_CreateDatabaseCommand:
			return f.applyCreateDatabaseCommand(&cmd)
		case internal.Command_CreateRetentionPolicyCommand:
			return f.applyCreateRetentionPolicyCommand(&cmd)
		case internal.Command_UpdateDataNodeCommand:
			return f.applyUpdateDataNodeCommand(&cmd)
		case internal.Command_DropDatabaseCommand:
			return f.applyDropDatabaseCommand(&cmd)
		case internal.Command_DropRetentionPolicyCommand:
			return f.applyDropRetentionPolicy(&cmd)
		case internal.Command_CreateUserCommand:
			return f.applyCreateUser(&cmd)
		case internal.Command_UpdateUserCommand:
			return f.applyUpdateUser(&cmd)
		case internal.Command_DropUserCommand:
			return f.applyDropUser(&cmd)
		case internal.Command_SetAdminPrivilegeCommand:
			return f.applySetAdminPrivilege(&cmd)
		case internal.Command_SetPrivilegeCommand:
			return f.applySetPrivilege(&cmd)
		case internal.Command_DropShardCommand:
			return f.applyDropShard(&cmd)
		case internal.Command_CreateShardGroupCommand:
			return f.applyCreateShardGroup(&cmd)
		case internal.Command_DeleteShardGroupCommand:
			return f.applyDeleteShardGroup(&cmd)
		case internal.Command_CreateContinuousQueryCommand:
			return f.applyCreateContinuousQuery(&cmd)
		case internal.Command_DropContinuousQueryCommand:
			return f.applyDropContinuousQuery(&cmd)
		case internal.Command_CreateSubscriptionCommand:
			return f.applyCreateSubscription(&cmd)
		case internal.Command_DropSubscriptionCommand:
			return f.applyDropSubscription(&cmd)
		case internal.Command_UpdateRetentionPolicyCommand:
			return f.applyUpdateRetentionPolicy(&cmd)
		default:
			return fmt.Errorf("can't apply command")
		}
		return nil
	}()
	f.data.Term = l.Term
	f.data.Index = l.Index

	close(f.datachanged)
	f.datachanged = make(chan struct{})
	return err
}
func (f *FSMStore) applyUpdateRetentionPolicy(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_UpdateRetentionPolicyCommand_Command)
	v := ext.(*internal.UpdateRetentionPolicyCommand)
	other := f.data.Clone()
	dur := time.Duration(v.GetDuration())
	rep := int(v.GetReplicaN())
	sdur := time.Duration(v.GetShardGroupDuration())
	rpu := &RetentionPolicyUpdate{
		Name:               proto.String(v.GetNewName()),
		Duration:           &dur,
		ReplicaN:           &rep,
		ShardGroupDuration: &sdur,
	}
	if err := other.UpdateRetentionPolicy(v.GetDatabase(), v.GetName(), rpu, v.GetMakeDefault()); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyDropSubscription(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_DropSubscriptionCommand_Command)
	v := ext.(*internal.DropSubscriptionCommand)
	other := f.data.Clone()
	if err := other.DropSubscription(v.GetDatabase(), v.GetRetentionPolicy(), v.GetName()); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyCreateSubscription(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateSubscriptionCommand_Command)
	v := ext.(*internal.CreateSubscriptionCommand)
	other := f.data.Clone()
	if err := other.CreateSubscription(v.GetDatabase(), v.GetRetentionPolicy(), v.GetName(), v.GetMode(), v.GetDestinations()); err != nil {
		return err
	}
	f.data = other
	return nil

}
func (f *FSMStore) applyDropContinuousQuery(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_DropContinuousQueryCommand_Command)
	v := ext.(*internal.DropContinuousQueryCommand)
	other := f.data.Clone()
	if err := other.DropContinuousQuery(v.GetDatabase(), v.GetName()); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyCreateContinuousQuery(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateContinuousQueryCommand_Command)
	v := ext.(*internal.CreateContinuousQueryCommand)
	other := f.data.Clone()
	if err := other.CreateContinuousQuery(v.GetDatabase(), v.GetName(), v.GetQuery()); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyDeleteShardGroup(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_DeleteShardGroupCommand_Command)
	v := ext.(*internal.DeleteShardGroupCommand)
	other := f.data.Clone()
	if err := other.DeleteShardGroup(v.GetDatabase(), v.GetPolicy(), v.GetShardGroupID()); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyCreateShardGroup(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateShardGroupCommand_Command)
	v := ext.(*internal.CreateShardGroupCommand)
	other := f.data.Clone()
	if err := other.CreateShardGroup(v.GetDatabase(), v.GetPolicy(), time.Unix(0, v.GetTimestamp())); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyDropShard(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_DropShardCommand_Command)
	v := ext.(*internal.DropShardCommand)
	other := f.data.Clone()
	other.DropShard(v.GetID())
	f.data = other
	return nil
}
func (f *FSMStore) applySetPrivilege(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_SetPrivilegeCommand_Command)
	v := ext.(*internal.SetPrivilegeCommand)
	other := f.data.Clone()
	if err := other.SetPrivilege(v.GetUsername(), v.GetDatabase(), influxql.Privilege(v.GetPrivilege())); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applySetAdminPrivilege(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_SetAdminPrivilegeCommand_Command)
	v := ext.(*internal.SetAdminPrivilegeCommand)
	other := f.data.Clone()
	if err := other.SetAdminPrivilege(v.GetUsername(), v.GetAdmin()); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyDropUser(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_DropUserCommand_Command)
	v := ext.(*internal.DropUserCommand)
	other := f.data.Clone()
	if err := other.DropUser(v.GetName()); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyUpdateUser(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_UpdateUserCommand_Command)
	v := ext.(*internal.UpdateUserCommand)
	other := f.data.Clone()
	if err := other.UpdateUser(v.GetName(), v.GetHash()); err != nil {
		return err
	}
	return nil
}
func (f *FSMStore) applyCreateUser(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateUserCommand_Command)
	v := ext.(*internal.CreateUserCommand)
	other := f.data.Clone()
	if err := other.CreateUser(v.GetName(), v.GetHash(), v.GetAdmin()); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyDropRetentionPolicy(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_DropRetentionPolicyCommand_Command)
	v := ext.(*internal.DropRetentionPolicyCommand)
	other := f.data.Clone()
	if err := other.DropRetentionPolicy(v.GetDatabase(), v.GetName()); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyDropDatabaseCommand(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_DropDatabaseCommand_Command)
	v := ext.(*internal.DropDatabaseCommand)
	other := f.data.Clone()
	if err := other.DropDatabase(v.GetName()); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyCreateDatabaseCommand(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateDatabaseCommand_Command)
	v := ext.(*internal.CreateDatabaseCommand)

	other := f.data.Clone()
	if err := other.CreateDatabase(v.GetName()); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyCreateRetentionPolicyCommand(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_CreateRetentionPolicyCommand_Command)
	v := ext.(*internal.CreateRetentionPolicyCommand)
	other := f.data.Clone()

	mrpi := &RetentionPolicyInfo{}
	mrpi.unmarshal(v.GetRetentionPolicy())
	if err := other.CreateRetentionPolicy(v.GetDatabase(), mrpi, v.GetMakeDefault()); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) applyUpdateDataNodeCommand(cmd *internal.Command) error {
	ext, _ := proto.GetExtension(cmd, internal.E_UpdateDataNodeCommand_Command)
	v := ext.(*internal.UpdateDataNodeCommand)
	other := f.data.Clone()
	nodeInfo := NodeInfo{
		ID:      v.GetID(),
		Host:    v.GetHost(),
		TCPHost: v.GetTCPHost(),
	}
	if err := other.UpdateDataNode(nodeInfo); err != nil {
		return err
	}
	f.data = other
	return nil
}
func (f *FSMStore) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	o := f.data.Clone()
	f.mu.RUnlock()
	return &fsmSnapshot{data: o}, nil
}

func (f *FSMStore) Restore(rc io.ReadCloser) error {
	b, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}
	data := &Data{}
	if err := data.UnmarshalBinary(b); err != nil {
		return err
	}
	f.data = data
	return nil
}

type fsmSnapshot struct {
	data *Data
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := f.data.MarshalBinary()
		if err != nil {
			return err
		}

		//write data to sink
		if _, err := sink.Write(b); err != nil {
			return err
		}

		//close the sink
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
		return err
	}
	return nil
}

func (f *fsmSnapshot) Release() {}
