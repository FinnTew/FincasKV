package meta

import "sync"

type Metadata struct {
	mu       sync.RWMutex
	NodeID   string            `json:"node_id"`
	NodeAddr string            `json:"node_addr"`
	Peers    map[string]string `json:"peers"` // NodeID -> Address
}

func New() *Metadata {
	return &Metadata{
		Peers: make(map[string]string),
	}
}

func (m *Metadata) Clone() *Metadata {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make(map[string]string)
	for k, v := range m.Peers {
		peers[k] = v
	}

	return &Metadata{
		NodeID:   m.NodeID,
		NodeAddr: m.NodeAddr,
		Peers:    peers,
	}
}

func (m *Metadata) AddPeer(nodeID, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Peers[nodeID] = addr
}

func (m *Metadata) RemovePeer(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.Peers, nodeID)
}
