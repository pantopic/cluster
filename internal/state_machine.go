package internal

import (
	"context"
	"fmt"
	"io"

	"github.com/logbn/fastjson-valid"
	"github.com/logbn/zongzi"
)

const StateMachineUri = "pantopic/cluster/internal"

func StateMachineFactory(logger Logger) func(shardID, replicaID uint64) zongzi.StateMachine {
	return func(shardID, replicaID uint64) zongzi.StateMachine {
		return &stateMachine{
			log:       logger,
			shardID:   shardID,
			replicaID: replicaID,
			state:     newState(),
		}
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

var parser fastjson.ValidParserPool

func (fsm *stateMachine) Update(entries []Entry) []Entry {
	var state *State
	var err error
	var cmd *fastjson.Value
	for i, entry := range entries {
		state = fsm.state.withTxn(true)
		if len(entry.Cmd) < 1 {
			fsm.log.Errorf(ErrCommandEmpty.Error())
			continue
		}
		p := parser.Get()
		defer parser.Put(p)
		cmd, err = p.ParseBytes(entry.Cmd)
		if err != nil {
			entry.Result.Value = ResultErrSyntactic
			fsm.log.Errorf(`invalid command (%s): %s`, err.Error(), string(entry.Cmd))
			continue
		}
		cmdType := fmt.Sprintf("%s.%s",
			cmd.GetStringBytes(`type`),
			cmd.GetStringBytes(`meth`))
		cmdFunc, ok := commands[cmdType] // see command.go
		if !ok {
			entry.Result.Value = ResultErrSemantic
			fsm.log.Errorf(`unrecognized command (%s): %s`, cmdType, string(entry.Cmd))
			continue
		}
		err = cmdFunc(&entry, state, cmd.Get(`data`))
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
	p := parser.Get()
	defer parser.Put(p)
	query, err := p.ParseBytes(data)
	if err != nil {
		result.Value = ResultErrSyntactic
		fsm.log.Errorf(`invalid query (%s): %s`, err.Error(), string(data))
		return
	}
	qType := fmt.Sprintf(`%s.%s`,
		query.GetStringBytes(`type`),
		query.GetStringBytes(`meth`))
	queryFunc, ok := queries[qType] // see query.go
	if !ok {
		result.Value = ResultErrSemantic
		fsm.log.Errorf(`unrecognized query (%s): %s`, qType, string(data))
		return
	}
	err = queryFunc(result, fsm.state.withTxn(false), query.Get(`data`))
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
