package main

import (
	"flag"
	"fmt"
	"github.com/jasonlvhit/gocron"
	"oakleaf/cluster"
	//"oakleaf/config"
	//"oakleaf/cluster/node/client"
	"oakleaf/cluster/node/server"
	"oakleaf/config"
	"oakleaf/console"
	"oakleaf/heartbeat"
	"oakleaf/storage"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

import (
	_ "net/http/pprof"
	"github.com/darkworon/oakleaf/cluster/balancing"
)

//go:generate go-bindata -nomemcopy html/...

type omit *struct{}

var files = storage.Files

//var conf2 = cluster.Config{}

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
	balancePeriod         = 30 * time.Second
	defaultUplinkRatio    = 1048576 * 10 // 1 MB/s * 10
	defaultDownlinkRatio  = 1048576 * 1  // 1 MB/s * 10
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
	initNode             bool
)

var memprofile = "./oakleaf1.mprof"

func nodeInit() {
	cluster.Init()
}

func JoinCluster(n string) {

}

func init() {
	var conf = config.Get()
	fmt.Println("[INFO] Oakleaf server node is initializing...")
	flag.BoolVar(&initNode, "init", false, "init node config")
	if !initNode {
		//if err != nil {
		flag.StringVar(&workingDirectory, "dir", defaultWorkingDir, "working directory")
		flag.IntVar(&conf.NodePort, "port", defaultPort, "node server port")
		flag.IntVar(&conf.ReplicaCount, "r", defaultReplicaCount, "parameter sets replication count")
		flag.StringVar(&nodeName, "name", defaultNodeName, "node name*")
		flag.Int64Var(&conf.UplinkRatio, "up", defaultUplinkRatio, "uplink speed")
		flag.Int64Var(&conf.PartChunkSize, "chunk", chunkSize, "files upload chunking size (in bytes)")
		flag.Int64Var(&conf.DownlinkRatio, "down", defaultDownlinkRatio, "downlink speed")
		flag.StringVar(&conf.ConfigFile, "conf", configFileName, "config files name")
		flag.StringVar(&conf.IndexFile, "index", indexFileName, "index files name")
		flag.BoolVar(&conf.UseTLS, "tls", false, "use TLS or not")
	}
	flag.Parse()
	if flag.Args() != nil {
		//nearNode = flag.Args()[0]
		for _, x := range flag.Args() {
			conf.ClusterNodes = append(conf.ClusterNodes, config.NodeAddress(x))
		}
	}

	//}
	/*if workingDirectory[:1] != "/" {
		workingDirectory += "/"
	}*/
	conf.WorkingDir = workingDirectory
	conf.DataDir = filepath.Join(workingDirectory, defaultDataStorageDir) //workingDirectory + defaultDataStorageDir + "/"

	os.MkdirAll(conf.DataDir, os.ModePerm)

	fmt.Println("Working directory: " + workingDirectory)
	fmt.Println("Data storage directory: " + conf.DataDir)
	fmt.Println("Node name: " + conf.NodeName)
	fmt.Println("Node port: " + strconv.Itoa(conf.NodePort))
	fmt.Println("Replication count: " + strconv.Itoa(conf.ReplicaCount))
	err := config.Import(conf.WorkingDir, configFileName)
	if err != nil {

		//HandleError(err)
	}
	nodeInit()
	fmt.Println("I have " + strconv.Itoa(storage.PartsCount(conf)) + " parts!")
	files.Import(workingDirectory, indexFileName)

	//(nodes.CurrentNode(conf)).PartsCount = storage.PartsCount(conf)
	//var usedSpace int64
	//usedSpace, err = utils.DirSize(conf.DataDir)
	//(nodes.CurrentNode(conf)).UsedSpace = usedSpace
	if nodeName != defaultNodeName {
		n := cluster.CurrentNode()
		n.Name = nodeName
		conf.NodeName = nodeName
	}

}

func main() {
	conf := config.Get()
	defer config.Save()
	server.Start(conf.NodePort)
	heartbeat.Start(heartBeatPeriod, conf)
	go balancing.Worker(balancePeriod)
	_, time := gocron.NextRun()
	fmt.Println(time)
	go func() { <-gocron.Start() }()
	config.Save()
	console.Worker()

}
