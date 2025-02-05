package fsm

import (
	"encoding/json"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/cluster/command"
	"github.com/FinnTew/FincasKV/internal/database"
	"github.com/hashicorp/raft"
	"io"
)

type FSM struct {
	db *database.FincasDB
}

func New(db *database.FincasDB) *FSM {
	return &FSM{db: db}
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd command.BaseCmd
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	c := command.New(cmd.GetType(), cmd.GetMethod(), cmd.Args)
	if err := c.Apply(f.db); err != nil {
		return fmt.Errorf("failed to apply command: %w", err)
	}

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &Snapshot{}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	return nil
}

type Snapshot struct{}

func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (s *Snapshot) Release() {}
