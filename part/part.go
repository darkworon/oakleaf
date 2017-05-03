package part

import (
	"time"
)

type Unit struct {
	ID             string    `json:"id"`
	Size           int64     `json:"size"`
	CreatedAt      time.Time `json:"created_at"`
	MainNodeID     string    `json:"main_nodeID"`
	ReplicaNodeID  string    `json:"replica_nodeID,omitempty"`
	ReplicaNodesID []string  `json:"replica_nodesID,omitempty"`
}
