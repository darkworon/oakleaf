package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"oakleaf/utils"
	"path/filepath"
)

type Config struct {
	ConfigInterface
	NodeName      string   `json:"node_name"`
	NodeID        string   `json:"node_id"`
	NodePort      int      `json:"node_port"`
	WorkingDir    string   `json:"working_dir"`
	DataDir       string   `json:"data_dir"`
	ReplicaCount  int      `json:"replica_count,omitempty"`
	PartChunkSize int      `json:"chunk_size,omitempty"`
	ClusterNodes  []string `json:"cluster_nodes,omitempty"`
	DownlinkRatio int64
	UplinkRatio   int64
	ConfigFile    string
	IndexFile     string
}

type ConfigInterface interface {
	Import(string, string) error
	Save()
}

func (c *Config) Import(dir, fName string) error {
	fmt.Println(dir, fName)
	var _configJson, err = utils.LoadExistingFile(filepath.Join(dir, fName))
	if err != nil {
		utils.HandleError(err)
		return err
	}
	err = json.Unmarshal(_configJson, *c)
	utils.HandleError(err)
	//if err == nil {
	//fmt.Println("ZZZHAHAHAHAHA, " + NodeConfig.NodeID)
	//CurrentNode = Nodes.FindNode(NodeConfig.NodeID)
	//if CurrentNode == nil {
	//fmt.Println("UHAHAHAHAHAHAHAHA, " + NodeConfig.NodeID)
	//node := &Node{NodeConfig.NodeID, NodeConfig.NodeName, "127.0.0.1:" + strconv.Itoa(NodeConfig.NodePort), true, 32212254720, 0, 0, 0, time.Now()}
	//	Nodes = append(Nodes, node)
	//nodesInfoWorker()
	//	}
	//nodesList = NodeConfig.ClusterNodes
	//refreshNodesList()
	//}
	return err
}

func (c *Config) Save() {
	configJson, _ := json.Marshal(c)
	ioutil.WriteFile(filepath.Join(c.WorkingDir, c.ConfigFile), []byte(utils.JsonPrettyPrint(string(configJson))), 0644)
}

var NodeConfig = Config{}
