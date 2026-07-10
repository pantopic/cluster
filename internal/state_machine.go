package internal

import (
	"context"
	"io"

	"github.com/logbn/zongzi"
)

const StateMachineUri = "pantopic/cluster/internal"

func StateMachineFactory(logger Logger, ctrlSvc ControllerService) func(shardID, replicaID uint64) zongzi.StateMachine {
	return func(shardID, replicaID uint64) zongzi.StateMachine {
		fsm := &stateMachine{
			log:       logger,
			shardID:   shardID,
			replicaID: replicaID,
			state:     newState(),
		}
		ctrlSvc.SetState(fsm.state)
		return fsm
	}
}

var _ zongzi.StateMachine = (*stateMachine)(nil)

type stateMachine struct {
	zongzi.StateMachine

	log       Logger
	shardID   uint64
	replicaID uint64
	state     *State
}

func (fsm *stateMachine) Update(entries []Entry) []Entry {
	var state *State
	var err error
	for i, entry := range entries {
		state = fsm.state.withTxn(true)
		if len(entry.Cmd) < 1 {
			state.abort()
			fsm.log.Errorf(ErrCommandEmpty.Error())
			continue
		}
		cmdType := entry.Cmd[len(entry.Cmd)-1]
		cmdFunc, ok := commands[cmdType] // see cmd.go
		if !ok {
			state.abort()
			entry.Result.Value = ResultErrSemantic
			fsm.log.Errorf(`unrecognized command (%d): %s`, cmdType, string(entry.Cmd))
			continue
		}
		err = cmdFunc(&entry, state, entry.Cmd[:len(entry.Cmd)-1])
		if err != nil {
			fsm.log.Warningf(err.Error())
			state.abort()
		} else {
			state.metaSetIndex(entry.Index)
			state.commit()
		}
		entries[i] = entry
	}
	return entries
}

func (fsm *stateMachine) Query(ctx context.Context, data []byte) (result *Result) {
	result = &Result{}
	qType := data[len(data)-1]
	queryFunc, ok := queries[qType] // see query.go
	if !ok {
		result.Value = ResultErrSemantic
		fsm.log.Errorf(`unrecognized query (%d): %s`, qType, string(data))
		return
	}
	state := fsm.state.withTxn(false)
	defer state.abort()
	queryFunc(result, state, data[:len(data)-1])
	return
}

func (fsm *stateMachine) PrepareSnapshot() (cursor any, err error) {
	return fsm.state.withTxn(false), nil
}

func (fsm *stateMachine) SaveSnapshot(cursor any, w io.Writer, _ zongzi.SnapshotFileCollection, close <-chan struct{}) (err error) {
	return cursor.(*State).Save(w)
}

func (fsm *stateMachine) RecoverFromSnapshot(r io.Reader, _ []zongzi.SnapshotFile, _ <-chan struct{}) error {
	return fsm.state.recover(r)
}

func (fsm *stateMachine) Lookup(any) (res any, err error) { return }

func (fsm *stateMachine) Close() (err error) { return }

func (fsm *stateMachine) Txn(write bool) *State {
	return fsm.state.withTxn(write)
}
