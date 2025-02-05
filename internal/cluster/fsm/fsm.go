package fsm

import (
	"encoding/json"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/cluster/command"
	"github.com/FinnTew/FincasKV/internal/cluster/meta"
	"github.com/FinnTew/FincasKV/internal/database"
	"github.com/hashicorp/raft"
	"sync"
)

type FSM struct {
	db       *database.FincasDB
	mu       sync.RWMutex
	metadata *meta.Metadata
}

func New(db *database.FincasDB) *FSM {
	return &FSM{
		db:       db,
		metadata: meta.New(),
	}
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd command.BaseCmd
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	c := command.New(cmd.GetType(), cmd.GetMethod(), cmd.Args)
	return c.Apply(f.db)
}
