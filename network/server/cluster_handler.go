package server

import (
	"encoding/json"
	"fmt"
	"github.com/FinnTew/FincasKV/cluster/node"
	"github.com/FinnTew/FincasKV/network/conn"
	"github.com/FinnTew/FincasKV/network/protocol"
	"strings"
)

type clusterReq struct {
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
}

func (s *Server) handleClusterCommand(conn *conn.Connection, cmd *protocol.Command) error {
	switch strings.ToUpper(cmd.Name) {
	case "CLUSTER":
		if len(cmd.Args) < 1 {
			return conn.WriteError(fmt.Errorf("CLUSTER command requires subcommand"))
		}

		switch strings.ToUpper(string(cmd.Args[0])) {
		case "INIT":
			if s.node != nil {
				return conn.WriteError(fmt.Errorf("cluster already initialized"))
			}

			if len(cmd.Args) != 3 { // CLUSTER INIT <node-id> <raft-addr>
				return conn.WriteError(fmt.Errorf("CLUSTER INIT requires node-id and raft-addr"))
			}

			nodeID := string(cmd.Args[1])
			raftAddr := string(cmd.Args[2])

			conf := &node.Config{
				NodeID:    nodeID,
				RaftBind:  raftAddr,
				RaftDir:   fmt.Sprintf("raft-data/%s", nodeID),
				Bootstrap: true,
			}

			if err := s.initCluster(conf); err != nil {
				return conn.WriteError(fmt.Errorf("failed to initialize cluster: %v", err))
			}

			return conn.WriteString("OK")

		case "JOIN":
			if s.node == nil {
				return conn.WriteError(fmt.Errorf("cluster not initialized"))
			}

			if len(cmd.Args) != 3 { // CLUSTER JOIN <node-id> <raft-addr>
				return conn.WriteError(fmt.Errorf("CLUSTER JOIN requires node-id and raft-addr"))
			}

			nodeID := string(cmd.Args[1])
			addr := string(cmd.Args[2])

			if err := s.node.Join(nodeID, addr); err != nil {
				return conn.WriteError(fmt.Errorf("failed to join cluster: %v", err))
			}

			return conn.WriteString("OK")

		case "INFO":
			if s.node == nil {
				return conn.WriteError(fmt.Errorf("cluster not initialized"))
			}

			info := map[string]interface{}{
				"node_id":     s.node.ID(),
				"is_leader":   s.node.IsLeader(),
				"leader_addr": s.node.GetLeaderAddr(),
			}

			data, err := json.Marshal(info)
			if err != nil {
				return conn.WriteError(fmt.Errorf("failed to marshal cluster info: %v", err))
			}

			return conn.WriteBulk(data)

		default:
			return conn.WriteError(fmt.Errorf("unknown cluster command: %s", cmd.Args[0]))
		}
	}

	return nil
}
