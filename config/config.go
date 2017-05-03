package config

type Config struct {
	NodeName      string   `json:"node_name"`
	NodeID        string   `json:"node_id"`
	NodePort      int      `json:"node_port"`
	WorkingDir    string   `json:"working_dir"`
	DataDir       string   `json:"data_dir"`
	ReplicaCount  int      `json:"replica_count,omitempty"`
	PartChunkSize int      `json:"chunk_size,omitempty"`
	ClusterNodes  []string `json:"cluster_nodes,omitempty"`
}
