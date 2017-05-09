package parts

import (
	"time"
)

type Part struct {
	ID        string    `json:"id"`
	Size      int64     `json:"size"`
	CreatedAt time.Time `json:"created_at"`
	Nodes     []string  `json:"nodes"`
}

type PartUploadOptions struct {
	PartID         string   `url:"partID"`
	Replica        bool     `url:"replica,omitempty"`
	Move           bool     `url:"move,omitempty"`
	MainNodeID     string   `url:"mainNode"`
	ReplicaNodesID []string `url:"replicaNode,omitempty"`
	Size           int64        `url:"size,omitempty"`
}

type ChangeNode struct {
	PartID    string `url:"partID"`
	OldNodeID string `url:"old_nodeID"`
	NewNodeID string `url:"new_nodeID"`
}
