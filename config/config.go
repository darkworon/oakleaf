package config

import (
	"encoding/json"
	"io/ioutil"
	"oakleaf/utils"
	"path/filepath"
	"errors"
)


type Config struct {
	ConfigInterface                `json:"-"`
	NodeName      string        `json:"node_name"`
	NodeID        string        `json:"node_id"`
	NodePort      int        `json:"node_port"`
	WorkingDir    string        `json:"working_dir"`
	DataDir       string        `json:"data_dir"`
	ReplicaCount  int        `json:"replica_count,omitempty"`
	PartChunkSize int64        `json:"chunk_size,omitempty"`
	ClusterNodes  AddressList        `json:"cluster_nodes,omitempty"`
	DownlinkRatio int64        `json:"downlink_ratio"`
	UplinkRatio   int64        `json:"uplink_ratio"`
	ConfigFile    string        `json:"-"`
	IndexFile     string        `json:"-"`
	UseTLS        bool        `json:"use_tls"`
}

var ErrNotInitialized = errors.New("Error: config not initialized yet.")

type ConfigInterface interface {
	Import(string, string) error
	Save()
}

func New() *Config {
	return &Config{}
}

func Get() *Config {
	if conf == nil {
		utils.HandleError(ErrNotInitialized)
		return nil
	}
	return conf
}

func Import(dir, fName string) error {
	//fmt.Println(dir, fName)
	var _configJson, err = utils.LoadExistingFile(filepath.Join(dir, fName))
	if err != nil {
		utils.HandleError(err)
		Save()
		return err
	}
	err = json.Unmarshal(_configJson, &conf)
	utils.HandleError(err)
	return err
}

func (c *Config) Protocol() string {
	if c.UseTLS {
		return "https"
	} else {
		return "http"
	}
}

func Save() {
	configJson, _ := json.Marshal(conf)
	ioutil.WriteFile(filepath.Join(conf.WorkingDir, conf.ConfigFile), []byte(utils.JsonPrettyPrint(string(configJson))), 0644)
}

func (c *Config) NodeExists(n NodeAddress) bool {
	for _, x := range c.ClusterNodes {
		if x == n {
			return true
		}
	}
	return false
}

var NodeConfig = New()
var conf = NodeConfig

var ShuttingDown = false