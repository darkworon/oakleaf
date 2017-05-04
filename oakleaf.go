package main

import (
	"flag"
	"fmt"
	"github.com/ventu-io/go-shortid"
	"oakleaf/cluster"
	//"oakleaf/config"
	"oakleaf/console"
	"oakleaf/heartbeat"
	"oakleaf/node/server"
	"oakleaf/storage"
	"oakleaf/utils"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

import _ "net/http/pprof"

/*
func (cs *ConcurrentSlice) Iter() <-chan ConcurrentSliceItem {
	c := make(chan ConcurrentSliceItem)

	f := func() {
		cs.Lock()
		defer cs.Lock()
		for index, value := range cs.items {
			c <- ConcurrentSliceItem{index, value}
		}
		close(c)
	}
	go f()

	return c
}*/

type omit *struct{}

var files = storage.Files
var nodes = cluster.Nodes
var conf = storage.Config

var conf2 = cluster.Config{}

const (
	chunkSize             = 52428800 // 500 mbytes
	defaultWorkingDir     = "node1"
	defaultNodeName       = "oakleafnode"
	defaultDataStorageDir = "data"
	defaultPort           = 8085
	defaultReplicaCount   = 1
	configFileName        = "config.json"
	indexFileName         = "files.json"
	heartBeatPeriod       = 1 * time.Second // ms
	defaultUplinkRatio    = 1048576         // 1 MB/s
	defaultDownlinkRatio  = 1048576         // 1 MB/s
)

var (
	workingDirectory     string
	dataStorageDirectory string
	nodePort             int
	replicaCount         int
	nodeName             string
	nodesList            []string
	nearNode             string
	downlinkRatio        int64
	uplinkRatio          int64
)

var memprofile = "./oakleaf1.mprof"

func nodeInit() {
	if nodes.Count() <= 0 {
		id, _ := shortid.Generate()
		//node = &cluster.Node{id, nodeName, "127.0.0.1:" + strconv.Itoa(NodeConfig.NodePort), true, 32212254720, 0, 0, 0, time.Now()}
		node := cluster.NewNode(id, nodeName, "127.0.0.1:"+strconv.Itoa(conf.NodePort), 32212254720, 0)
		nodes.Add(node)
	} else {
		node := nodes.CurrentNode(conf)
		conf.NodeID = node.ID
		conf.NodeName = node.Name
	}

	//NodeConfig.ClusterNodes = nodesList
	time.Sleep(1 * time.Second)
	nodes.RefreshNodesList(conf)
}

func JoinCluster(n string) {

}

func init() {

	//if err != nil {
	flag.StringVar(&workingDirectory, "dir", defaultWorkingDir, "working directory")
	flag.IntVar(&conf.NodePort, "port", defaultPort, "node server port")
	flag.IntVar(&conf.ReplicaCount, "r", defaultReplicaCount, "parameter sets replication count")
	flag.StringVar(&nodeName, "name", defaultNodeName, "node name*")
	flag.Int64Var(&uplinkRatio, "up", defaultUplinkRatio, "uplink speed")
	flag.Int64Var(&downlinkRatio, "down", defaultDownlinkRatio, "downlink speed")
	flag.StringVar(&conf.ConfigFile, "conf", configFileName, "config file name")
	flag.StringVar(&conf.IndexFile, "index", indexFileName, "index file name")
	flag.Parse()
	if flag.Args() != nil {
		//nearNode = flag.Args()[0]
		conf.ClusterNodes = flag.Args()
	}

	//}
	/*if workingDirectory[:1] != "/" {
		workingDirectory += "/"
	}*/
	conf.DataDir = filepath.Join(workingDirectory, defaultDataStorageDir) //workingDirectory + defaultDataStorageDir + "/"
	fmt.Println("Working directory: " + workingDirectory)
	fmt.Println("Data storage directory: " + conf.DataDir)
	fmt.Println("Node name: " + conf.NodeName)
	fmt.Println("Node port: " + strconv.Itoa(conf.NodePort))
	fmt.Println("Replication count: " + strconv.Itoa(conf.ReplicaCount))

	os.MkdirAll(conf.DataDir, os.ModePerm)
	err := conf2.Import(conf.WorkingDir, configFileName)

	fmt.Println("[INFO] Oakleaf server node is initializing...")
	if err != nil {
		nodeInit()
		//HandleError(err)
	}
	fmt.Println("I have " + strconv.Itoa(storage.PartsCount(conf)) + " parts!")
	files.Import(workingDirectory, indexFileName)

	(nodes.CurrentNode(conf)).PartsCount = storage.PartsCount(conf)
	var usedSpace int64
	usedSpace, err = utils.DirSize(conf.DataDir)
	(nodes.CurrentNode(conf)).UsedSpace = usedSpace
	if nodeName != defaultNodeName {
		nodes.CurrentNode(conf).Name = nodeName
		conf.NodeName = nodeName
	}

}

func main() {
	defer conf.Save()
	//go fileNodeWorker(3801)
	//fmt.Println("it's skipped?")

	server.Start(conf.NodePort)
	heartbeat.Worker(heartBeatPeriod, conf)
	console.Worker()

}
