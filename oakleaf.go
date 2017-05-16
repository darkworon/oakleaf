package main

import (
	"flag"
	"oakleaf/cluster"
	"oakleaf/cluster/balancing"
	"oakleaf/cluster/node"
	"oakleaf/cluster/node/server"
	"oakleaf/config"
	"oakleaf/console"
	"oakleaf/files"
	"oakleaf/heartbeat"
	"oakleaf/logger"
	"oakleaf/parts/partstorage"
	"oakleaf/storage"
	"oakleaf/utils"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	_ "net/http/pprof"

	log "github.com/Sirupsen/logrus"
)

//go:generate go-bindata -nomemcopy html/...

type omit *struct{}

var fileList = files.All()

//var conf2 = cluster.Config{}

const (
	chunkSize             = 10485760 //52428800 = 50 mbytes
	defaultWorkingDir     = "node1"
	defaultNodeName       = "oakleafnode"
	defaultDataStorageDir = "data"
	defaultPort           = 8085
	defaultReplicaCount   = 1
	configFileName        = "config.json"
	indexFileName         = "files.json"
	partsFileName         = "parts.json"
	heartBeatPeriod       = 1 * time.Second // ms
	balancePeriod         = 5 * time.Second
	defaultUplinkRatio    = 1048576 * 2  // 2 MB/s
	defaultDownlinkRatio  = 1048576 * 1  // 1 MB/s
	defaultInterLinkRatio = 1048576 * 10 // 1 MB/s * 10
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
	logger.Initialize()
	var conf = config.Get()
	log.Infoln("[INFO] Oakleaf server node is initializing...")
	flag.BoolVar(&initNode, "init", false, "init node config")
	if !initNode {
		//if err != nil {
		flag.StringVar(&workingDirectory, "dir", defaultWorkingDir, "working directory")
		flag.IntVar(&conf.NodePort, "port", defaultPort, "node server port")
		flag.IntVar(&conf.ReplicaCount, "r", defaultReplicaCount, "parameter sets replication count")
		flag.StringVar(&nodeName, "name", defaultNodeName, "node name*")
		flag.Int64Var(&conf.UplinkRatio, "up", defaultUplinkRatio, "uplink speed limit")
		flag.Int64Var(&conf.DownlinkRatio, "down", defaultDownlinkRatio, "download speed limit")
		flag.Int64Var(&conf.InterLinkRatio, "ilratio", defaultInterLinkRatio, "speed limit between nodes")
		flag.Int64Var(&conf.PartChunkSize, "chunk", chunkSize, "fileList upload chunking size (in bytes)")
		flag.StringVar(&conf.ConfigFile, "conf", configFileName, "config fileList name")
		flag.StringVar(&conf.IndexFile, "index", indexFileName, "index fileList name")
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

	log.Infoln("Working directory: " + workingDirectory)
	log.Infoln("Data storage directory: " + conf.DataDir)
	log.Infoln("Node name: " + conf.NodeName)
	log.Infoln("Node port: " + strconv.Itoa(conf.NodePort))
	log.Infoln("Replication count: " + strconv.Itoa(conf.ReplicaCount))
	err := config.Import(conf.WorkingDir, configFileName)
	if err != nil {

		//HandleError(err)
	}
	nodeInit()
	log.Infof("I have %d parts", storage.PartsCount(conf))

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
	ec := make(chan os.Signal, 2)
	signal.Notify(ec, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ec
		log.Infoln("Exiting...")
		cluster.CurrentNode().IsActive = false
		cluster.CurrentNode().SetStatus(node.StateShuttingDown)
		config.ShuttingDown = true
		if cluster.AllActive().Count() > config.Get().ReplicaCount {
			//delete index file if i'm not alone... ask it after join from another node
			//log.Infoln("Removing index file...")
			//os.Remove(filepath.Join(config.Get().WorkingDir, indexFileName))
			balancing.MoveAllData()
		}
		config.Save()
		server.Stop <- true
		close(server.Stop)
		log.Infoln("Awaiting all processes done...")
		<-server.Stopped
		partstorage.Load()
		balancing.MoveAllData()
		time.Sleep(3 * time.Second)
		os.Exit(1)
	}()
	conf := config.Get()
	defer config.Save()
	server.Start(conf.NodePort)
	partsCount, err := storage.Import(workingDirectory, partsFileName)
	if err != nil || (partsCount < 1 && utils.DirSize(config.Get().DataDir) > 0) {
		partstorage.Load()
	}
	files.LoadFromCluster()
	heartbeat.Start(heartBeatPeriod, conf)
	go balancing.Worker(balancePeriod)
	config.Save()
	console.Worker()
	partstorage.Load()
}
