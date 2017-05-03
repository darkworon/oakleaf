package main

import (
	"bufio"
	"bytes"
	//"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	//"math"
	//"github.com/satori/go.uuid"
	"errors"
	"github.com/gorilla/mux"
	"github.com/ventu-io/go-shortid"
	//"httputil"
	"io"
	"io/ioutil"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	//"runtime"
	//"runtime/pprof"
	"github.com/google/go-querystring/query"
	//"log"
	"net/url"
	//"runtime/pprof"
	"strconv"
	"strings"
	"time"
)
import _ "net/http/pprof"

type Node struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Address    string    `json:"address"`
	IsActive   bool      `json:"is_active"`
	TotalSpace int64     `json:"total_space"`
	UsedSpace  int64     `json:"used_space"`
	FilesCount int       `json:"files_count"`
	PartsCount int       `json:"parts_count"`
	LastUpdate time.Time `json:"last_update"`
	//Parts      []string  `json:"parts,omitempty"`
}

type Part struct {
	ID             string    `json:"id"`
	Size           int64     `json:"size"`
	CreatedAt      time.Time `json:"created_at"`
	MainNodeID     string    `json:"main_nodeID"`
	ReplicaNodeID  string    `json:"replica_nodeID,omitempty"`
	ReplicaNodesID []string  `json:"replica_nodesID,omitempty"`
}

type File struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
	//Parts []*Part `json:"Parts"`
	Parts PartArr `json:"parts,omitempty"`
}

type PublicFile struct {
	*File
	Parts       omit   `json:"parts,omitempty"`
	DownloadURL string `json:"download_url`
}

type ServiceFile struct {
	*File
	*Node
}

type PublicNode struct {
	*Node
	LastUpdate omit `json:"last_update,omitempty"`
}

type PartUploadOptions struct {
	PartID         string   `url:"partID"`
	Replica        bool     `url:"replica,omitempty"`
	MainNodeID     string   `url:"mainNode"`
	ReplicaNodesID []string `url:"replicaNode,omitempty"`
}

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

type FileArr []*File
type PartArr []*Part
type NodeArr []*Node

type FilesSlice struct {
	sync.RWMutex
	List []*File
}

type NotificationSlice struct {
	sync.RWMutex
	List []*struct {
		*File
		*Node
	}
}

// Slice type that can be safely shared between goroutines
type ConcurrentSlice struct {
	sync.RWMutex
	items []interface{}
}

// Concurrent slice item
type ConcurrentSliceItem struct {
	Index int
	Value interface{}
}

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
}

type omit *struct{}

func (file *File) AddPart(part *Part) *File {
	file.Parts = append(file.Parts, part)
	return file
}

var Files = FilesSlice{}
var Parts = PartArr{}
var Nodes = NodeArr{}
var NodeConfig = Config{}
var CurrentNode = &Node{}

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

func (cs *ConcurrentSlice) Append(item interface{}) {
	cs.Lock()
	defer cs.Unlock()

	cs.items = append(cs.items, item)
}

func nodeInit() {
	var node *Node
	if len(Nodes) <= 0 {
		//fmt.Println("YHAHAHAHAHAHA")
		id, _ := shortid.Generate()
		node = &Node{id, nodeName, "127.0.0.1:" + strconv.Itoa(NodeConfig.NodePort), true, 32212254720, 0, 0, 0, time.Now()}
		Nodes = append(Nodes, node)
	} else {
		node = Nodes[0]
	}
	NodeConfig.NodeID = node.ID
	NodeConfig.NodeName = node.Name
	//NodeConfig.ClusterNodes = nodesList
	time.Sleep(1 * time.Second)
	refreshNodesList()
}

func loadConfigFile() error {
	var _configJson, err = loadExistingFile(workingDirectory + configFileName)
	if err != nil {
		return err
	}
	err = json.Unmarshal(_configJson, &NodeConfig)
	if err == nil {
		//fmt.Println("ZZZHAHAHAHAHA, " + NodeConfig.NodeID)
		CurrentNode = Nodes.FindNode(NodeConfig.NodeID)
		if CurrentNode == nil {
			//fmt.Println("UHAHAHAHAHAHAHAHA, " + NodeConfig.NodeID)
			node := &Node{NodeConfig.NodeID, NodeConfig.NodeName, "127.0.0.1:" + strconv.Itoa(NodeConfig.NodePort), true, 32212254720, 0, 0, 0, time.Now()}
			Nodes = append(Nodes, node)
			//nodesInfoWorker()
		}
		//nodesList = NodeConfig.ClusterNodes
		refreshNodesList()
	}
	return err
}

func (c *Config) NodeExists(n *Node) bool {
	for _, x := range c.ClusterNodes {
		if x == n.Address {
			return true
		}
	}
	return false
}

func (ns *NodeArr) NodeExists(n *Node) bool {
	for _, x := range Nodes {
		if x.ID == n.ID {
			return true
		}
	}
	return false
}

func (nl *NodeArr) GetCurrentNode() *Node {
	return Nodes.FindNode(NodeConfig.NodeID)
}

func addOrUpdateNodeInfo(node *Node) (joined bool) {
	//fmt.Println("7777")
	if !Nodes.NodeExists(node) {
		joined = true
		//	fmt.Println("88888")
		Nodes = append(Nodes, node)
		if !NodeConfig.NodeExists(node) {
			NodeConfig.ClusterNodes = append(NodeConfig.ClusterNodes, node.Address)
			//		fmt.Println("00000000")
		}
	} else if node.ID != Nodes.GetCurrentNode().ID {
		joined = false
		//	fmt.Println("9999")
		n := Nodes.FindNode(node.ID)
		if !n.IsActive {
			defer fmt.Printf("[CLUSTER] Node %s -> active\n", n.Address)
		}
		n.IsActive = node.IsActive
		n.TotalSpace = node.TotalSpace
		n.UsedSpace = node.UsedSpace
		n.FilesCount = node.FilesCount
		*n = *node
	}
	Nodes.Save()
	NodeConfig.Save()
	//nodesInfoWorker()
	return joined
}

func JoinCluster(n string) {

}

func refreshNodesList() {

	var wg sync.WaitGroup
	for _, n := range NodeConfig.ClusterNodes {
		wg.Add(1)
		go func(x string) {
			defer wg.Done()
			_node, err := nodeInfoExchange(x)
			if err != nil || _node == nil {
				HandleError(err)
			} else {
				addOrUpdateNodeInfo(_node)

			}
		}(n)
	}
	wg.Wait()
}

func nodeInfoExchange(address string) (node *Node, err error) {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		err := json.NewEncoder(w).Encode(Nodes.GetCurrentNode())
		if err != nil {
		}
	}()
	resp, err := http.Post(fmt.Sprintf("http://%s/node/info", address), "application/json; charset=utf-8", r)
	if err != nil {
		//HandleError(err)
		return nil, err
	}
	node = &Node{}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&node)
	return node, err
}

func nodeListHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	nodesJson, _ := json.Marshal(Nodes)
	w.Write([]byte(jsonPrettyPrint(string(nodesJson))))
}

func nodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	var node = &Node{}
	json.NewDecoder(r.Body).Decode(&node)

	if addOrUpdateNodeInfo(node) {
		fmt.Printf("[CLUSTER] Node %s joined the cluster\n", node.Address)
	}
	(Nodes.GetCurrentNode()).LastUpdate = time.Now()
	//fmt.Println(r.Body)
	nodeJson, err := json.Marshal(Nodes.GetCurrentNode())
	if err != nil {
		HandleError(err)
	}
	w.Write(nodeJson)

}

func nodesHeartbeatWorker() {
	time.Sleep(1 * time.Second)
	for {
		var wg sync.WaitGroup
		for _, x := range Nodes.AllExcept(Nodes.GetCurrentNode()) {
			//fmt.Println()
			wg.Add(1)
			//	if Nodes.FindNode(n) == nil {
			go func(n *Node) {
				defer wg.Done()
				//	fmt.Println(n)
				_node, err := nodeInfoExchange(n.Address)
				//fmt.Println("33333")
				if err != nil {
					//	HandleError(err)
					if n.IsActive {
						defer fmt.Printf("[CLUSTER] Node %s -> not active.\n", n.Address)
						n.IsActive = false
						//fmt.Println(n.IsActive)
					}
				} else if _node != nil {
					addOrUpdateNodeInfo(_node)
					//fmt.Println("555555")
				}
			}(x)
		}
		wg.Wait()
		time.Sleep(heartBeatPeriod)
	}
}

func (c *Config) Save() {
	configJson, _ := json.Marshal(NodeConfig)
	ioutil.WriteFile(workingDirectory+configFileName, []byte(jsonPrettyPrint(string(configJson))), 0644)
}
func HandleError(err error) (b bool) {
	if err != nil {
		// notice that we're using 1, so it will actually log where
		// the error happened, 0 = this function, we don't want that.
		_, fn, line, _ := runtime.Caller(1)
		log.Printf("[error] %s:%d %v", fn, line, err)
		b = true
	}
	return
}

func init() {

	//if err != nil {
	flag.StringVar(&workingDirectory, "dir", defaultWorkingDir, "working directory")
	flag.IntVar(&NodeConfig.NodePort, "port", defaultPort, "node server port")
	flag.IntVar(&NodeConfig.ReplicaCount, "r", defaultReplicaCount, "parameter sets replication count")
	flag.StringVar(&nodeName, "name", defaultNodeName, "node name*")
	flag.Int64Var(&uplinkRatio, "up", defaultUplinkRatio, "uplink speed")
	flag.Int64Var(&downlinkRatio, "down", defaultDownlinkRatio, "downlink speed")
	flag.Parse()
	if flag.Args() != nil {
		//nearNode = flag.Args()[0]
		NodeConfig.ClusterNodes = flag.Args()
	}

	//}
	if workingDirectory[:1] != "/" {
		workingDirectory += "/"
	}
	NodeConfig.DataDir = filepath.Join(workingDirectory, defaultDataStorageDir) //workingDirectory + defaultDataStorageDir + "/"
	fmt.Println("Working directory: " + workingDirectory)
	fmt.Println("Data storage directory: " + NodeConfig.DataDir)
	fmt.Println("Node name: " + NodeConfig.NodeName)
	fmt.Println("Node port: " + strconv.Itoa(NodeConfig.NodePort))
	fmt.Println("Replication count: " + strconv.Itoa(NodeConfig.ReplicaCount))

	os.MkdirAll(NodeConfig.DataDir, os.ModePerm)
	err := loadConfigFile()

	fmt.Println("[INFO] Oakleaf server node is initializing...")
	if err != nil {
		nodeInit()
		//HandleError(err)
	}
	fmt.Println("I have " + strconv.Itoa(getPartsCount()) + " parts!")
	_ = readIndexFile()

	(Nodes.GetCurrentNode()).PartsCount = getPartsCount()
	var usedSpace int64
	usedSpace, err = DirSize(NodeConfig.DataDir)
	(Nodes.GetCurrentNode()).UsedSpace = usedSpace
	if nodeName != defaultNodeName {
		(*Nodes.GetCurrentNode()).Name = nodeName
		NodeConfig.NodeName = nodeName
	}

}

func (na *NodeArr) GetLessLoadedNode() (n *Node) {
	var nodesListSorted NodeArr
	nodesListSorted = append(nodesListSorted, *na...)

	sort.Slice(&nodesListSorted, func(i, j int) bool {
		return nodesListSorted[i].UsedSpace < nodesListSorted[j].UsedSpace
	})
	return nodesListSorted[0]

}

func main() {
	defer NodeConfig.Save()
	//go fileNodeWorker(3801)
	//fmt.Println("it's skipped?")
	go func() {
		masterNodeWorker(NodeConfig.NodePort)
	}()
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	go func() {
		//time.Sleep(3 * time.Second)
		nodesHeartbeatWorker()
	}()
	//fmt.Println("yeah!it's skipped")

	consoleWorker()

}

func addTestFiles() {
	time.Sleep(2 * time.Second)
	ProcessFile("1.dmg")
	ProcessFile("1.mov")
}

func errorHandler(w http.ResponseWriter, r *http.Request, status int) {
	//w.WriteHeader(status)
	if status == http.StatusNotFound {
		http.Error(w, "404 - nothing found :(", status)
	}
	if status == http.StatusInternalServerError {
		http.Error(w, "500 Internal server error", status)
	}
}

func fileListHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	filesJson, _ := json.Marshal(Files.List)
	w.Write([]byte(jsonPrettyPrint(string(filesJson))))
}

func partsListHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	partsJson, _ := json.Marshal(Parts)
	w.Write([]byte(jsonPrettyPrint(string(partsJson))))
}

func fileDownloadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if vars["id"] != "" {
		var f = <-Files.Find(vars["id"])
		//w.Header().Set("Content-Length", string(f.Size))
		if f == nil {
			// not found file on this node - trying to get info from other nodes
			f = getFileInfoFromAllNodes(vars["id"])
		}
		if f.ID != "" {
			if f.IsAvailable() {
				w.Header().Set("Content-Description", "File Transfer")
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Content-Disposition", "attachment; filename="+f.Name)
				w.Header().Set("Content-Transfer-Encoding", "binary")
				w.Header().Set("Expires", "0")
				w.Header().Set("Cache-Control", "must-revalidate")
				w.Header().Set("Pragma", "public")
				w.Header().Set("Content-Length", fmt.Sprintf("%d", f.Size))
				w.WriteHeader(http.StatusOK)
				err := f.Download(&w, downlinkRatio)
				if err != nil {
					HandleError(err)
					fmt.Printf("[ERR] Can't serve file %s (%s) - not all nodes available\n", f.ID, f.Name)
				}
			} else {
				errorHandler(w, r, 500)
				fmt.Printf("[ERR] Can't serve file %s (%s) - not all nodes available\n", f.ID, f.Name)
			}
		} else {
			errorHandler(w, r, 404)
		}
		//fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
	}

}

func getFileInfoFromAllNodes(id string) *File {
	var f *File = new(File)
	for _, v := range Nodes {
		fmt.Printf("Trying to get file %s info from node %s...\n", id, v.Address)
		resp, err := http.Get(fmt.Sprintf("http://%s/file/info/%s", v.Address, id))
		if err != nil {
			HandleError(err)
			continue
		}
		defer resp.Body.Close()
		err = json.NewDecoder(resp.Body).Decode(&f)
		if err != nil {
			// if cant unmarshall - thinking we got no file info
			//HandleError(err)
			continue
		}
		if f != nil {
			//fmt.Println(f)
			fmt.Println("Found it! Adding to our list and sending back")
			//Files.List = append(Files.List, f)
			Files.Add(f)
			go updateIndexFiles()
			break
		}
	}
	return f
}

func fileInfoHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	var f = <-Files.Find(vars["id"])
	if f != nil {
		fileJson, _ := json.Marshal(f)
		w.WriteHeader(http.StatusOK)
		w.Write(fileJson)
	} else {
		errorHandler(w, r, 404)
	}
}

func partUploadHandler(w http.ResponseWriter, r *http.Request) {
	m, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		HandleError(err)
	}
	r.ParseMultipartForm(2 << 20)
	file, _, err := r.FormFile("data") // img is the key of the form-data
	defer file.Close()
	name, _ := shortid.Generate()
	//	fmt.Println(handler.Filename)
	if err != nil {
		HandleError(err)
	}
	var size int64 = 0
	//	fmt.Println(r.Header)
	//	r.Body = http.MaxBytesReader(w, r.Body, math.MaxInt64)
	if r.URL.Query().Get("replica") == "true" {
		//for replication - we stays original part ID/Name
		name = r.URL.Query().Get("partID")
		fmt.Printf("[PSINFO] Node %s sent to me replica of part %s\n", Nodes.FindNode(r.URL.Query().Get("mainNode")).Address, r.URL.Query().Get("partID"))
	}
	out, _ := os.OpenFile(filepath.Join(NodeConfig.DataDir, name), os.O_CREATE|os.O_WRONLY, 0666)
	defer out.Close()

	if err != nil {
		HandleError(err)
	}
	size, err = io.Copy(out, file)
	if err != nil && err != io.EOF {
		HandleError(err)
	}
	var p = &Part{
		ID:             name,
		Size:           size,
		MainNodeID:     r.URL.Query().Get("mainNode"),
		ReplicaNodesID: m["replicaNode"],
		CreatedAt:      time.Now(),
	}
	(Nodes.GetCurrentNode()).UsedSpace += p.Size
	(Nodes.GetCurrentNode()).PartsCount++
	partJson, _ := json.Marshal(p)
	w.Write([]byte(jsonPrettyPrint(string(partJson))))
	if r.URL.Query().Get("replica") != "true" {
		go func() {
			for _, x := range p.ReplicaNodesID {
				p.UploadCopies(Nodes.FindNode(p.MainNodeID), Nodes.FindNode(x))
			}
		}()
	}
	//Parts = append(Parts, p)
	//updateIndexFiles()

}

func (p *Part) UploadCopies(node1 *Node, node2 *Node) {
	fmt.Printf("[PSINFO] Main node: %s, uploading replica to the %s...\n", node1.Address, node2.Address)
	in, err := os.Open(filepath.Join(NodeConfig.DataDir, p.ID))
	if err != nil {
		HandleError(err)
	}
	defer in.Close()

	//	fstat, err := in.Stat()
	//	var fSize = fstat.Size()
	pr, pw := io.Pipe()
	mpw := multipart.NewWriter(pw)

	var size int64 = 0
	go func() {
		var part io.Writer
		defer pw.Close()

		if part, err = mpw.CreateFormFile("data", p.ID); err != nil && err != io.EOF {
			log.Fatal(err)
		}
		if size, err = io.Copy(part, in); err != nil && err != io.EOF {
			panic(err)

		}
		if err = mpw.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	opt := PartUploadOptions{
		PartID:     p.ID,
		MainNodeID: node1.ID,
		Replica:    true,
	}
	v, _ := query.Values(opt)

	resp, err := http.Post(fmt.Sprintf("http://%s/part?"+v.Encode(), node2.Address), mpw.FormDataContentType(), pr)
	if err != nil {
		HandleError(err)
	} else {
		p.ReplicaNodesID = append(p.ReplicaNodesID, p.ReplicaNodeID)
		go updateIndexFiles()
	}
	defer resp.Body.Close()
	//fmt.Println("Uploaded a replica!")
}

func (p *Part) FindNodesForReplication() (err error) {
	var nodesListSorted NodeArr
	nodesListSorted = append(nodesListSorted, Nodes...)

	sort.Slice(nodesListSorted, func(i, j int) bool {
		return (*nodesListSorted[i]).UsedSpace < (*nodesListSorted[j]).UsedSpace
	})
	foundNodes := 0
	//var replicaNode *Node
	for foundNodes < NodeConfig.ReplicaCount {
		for _, n := range nodesListSorted {
			if !p.CheckNodeExists(n) && n.IsActive {
				p.ReplicaNodesID = append(p.ReplicaNodesID, n.ID)
				foundNodes++
				break
			}
		}
		// can't find node for replica if comes here
	}
	return nil
}

func (p *Part) CheckNodeExists(node *Node) bool {
	if p.MainNodeID == node.ID {
		return true
	}
	for _, n := range p.ReplicaNodesID {
		if n == node.ID {
			return true
		}
	}
	return false
}

func fileUploadHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseMultipartForm(0)
	defer r.MultipartForm.RemoveAll()
	//r.Body = http.MaxBytesReader(w, r.Body, math.MaxInt64)
	r.ParseMultipartForm(2 << 20)
	file, handler, err := r.FormFile("data")
	//fmt.Println(r.ContentLength)

	if err != nil {
		HandleError(err)
		return
	}
	defer file.Close()

	var f = &File{}
	fileID, _ := shortid.Generate()
	f.ID = fileID
	f.Name = handler.Filename

	var fileSizeCounter int64 = 0
	for {
		var p Part
		p.ID, _ = shortid.Generate()
		pr, pw := io.Pipe()
		mpw := multipart.NewWriter(pw)

		in_r := io.LimitReader(file, int64(chunkSize))
		var size int64 = 0
		go func() {
			var part io.Writer
			defer pw.Close()

			if part, err = mpw.CreateFormFile("data", handler.Filename); err != nil && err != io.EOF {
				log.Fatal(err)
			}
			if size, err = io.Copy(part, in_r); err != nil && err != io.EOF {
				//	panic(err)
				HandleError(err)

			}
			if err = mpw.Close(); err != nil {
				HandleError(err)
			}
		}()
		var choosenNode *Node = findLessLoadedNode() //Nodes[rand.Intn(len(Nodes))]
		p.MainNodeID = choosenNode.ID
		p.FindNodesForReplication()
		opt := PartUploadOptions{
			PartID:         p.ID,
			MainNodeID:     choosenNode.ID,
			ReplicaNodesID: p.ReplicaNodesID,
		}

		v, _ := query.Values(opt)
		//fmt.Println(v.Encode())
		resp, err := http.Post(fmt.Sprintf("http://%s/part?"+v.Encode(), choosenNode.Address), mpw.FormDataContentType(), pr)
		if err != nil {
			HandleError(err)
		}
		defer resp.Body.Close()

		//fmt.Println(choosenNode.Address)

		err = json.NewDecoder(resp.Body).Decode(&p)

		if err != nil {
			HandleError(err)
		}
		fmt.Printf("Uploaded part %s to the node %s\n", p.ID, choosenNode.Address)
		//	p.ReplicaNodesID = append(p.ReplicaNodesID, replicaNode.ID)
		//		p.ReplicaNodeID = replicaNode.ID
		f.AddPart(&p)
		//Parts = append(Parts, &p)
		//partJson, _ := json.Marshal(p)
		//fmt.Println(partJson)
		//	replicaNode.FilesCount++
		//choosenNode.UsedSpace += p.Size
		//replicaNode.UsedSpace += p.Size
		fileSizeCounter += p.Size
		if p.Size < int64(chunkSize) {
			break
		}
	}

	//	f.Size = fileSizeCounter
	f.Size = fileSizeCounter
	//Files.List = append(Files.List, f)
	Files.Add(f)
	fileJson, _ := json.Marshal(PublicFile{
		File:        f,
		DownloadURL: fmt.Sprintf("http://%s/file/%s", Nodes.FindNode(NodeConfig.NodeID).Address, f.ID),
	})
	defer fmt.Printf("[INFO] Uploaded new file %s, %s\n", f.ID, f.Name)
	go updateIndexFiles()
	go newFileNotify(json.Marshal(f))
	w.Write([]byte(jsonPrettyPrint(string(fileJson))))
	/*if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
		return
	}*/
	fmt.Println(runtime.NumGoroutine())

}

func (nl *NodeArr) AllExcept(n *Node) NodeArr {
	var tempList = NodeArr{}
	for _, v := range *nl {
		if v.ID != n.ID {
			tempList = append(tempList, v)
		}
	}
	return tempList
}

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

func newFileNotify(jsonData []byte, err error) {
	var wg sync.WaitGroup
	//	fmt.Println(Nodes.GetCurrentNode())
	//fmt.Println(Nodes[1:])
	for _, v := range Nodes.AllExcept(Nodes.GetCurrentNode()) {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			if n.IsActive {
				resp, err := http.Post(fmt.Sprintf("http://%s/file/info", n.Address), "application/json", bytes.NewBuffer(jsonData))
				if err != nil {
					HandleError(err)
				}
				defer resp.Body.Close()
			}
		}(v)
		wg.Wait()
	}
}

func partDownloadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if vars["id"] != "" {
		//var p = Files.FindPart(vars["id"])
		w.Header().Set("Content-Disposition", "attachment; filename="+vars["id"])
		err := DownloadPart(&w, vars["id"])
		if err != nil && err != io.EOF {
			//errorHandler(w, r, 500)
			HandleError(err)
			fmt.Fprintf(w, "Not found part with id %s", vars["id"])
			fmt.Printf("[PSINFO] 404 - not found part \"%s\"\n", vars["id"])
		}
	}

}

func partDeleteHandler(w http.ResponseWriter, r *http.Request) {
}

func fileDeleteHandler(w http.ResponseWriter, r *http.Request) {
}

func getFileInfoHandler(w http.ResponseWriter, r *http.Request) {
	var file = &File{}
	err := json.NewDecoder(r.Body).Decode(&file)
	if err == nil {
		Files.Add(file)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	} else {
		errorHandler(w, r, 500)
	}

}

func (fl *FilesSlice) Add(f *File) {
	_f := <-fl.Find(f.ID)
	fl.Lock()
	defer fl.Unlock()
	if _f == nil {
		//		fmt.Println("IM HERE....", _f)
		fl.List = append(fl.List, f)
		//	(Nodes.FindNode(NodeConfig.NodeID)).FilesCount++
		(Nodes.FindNode(NodeConfig.NodeID)).FilesCount++
		//fmt.Println("Added new file: ", f.ID)
		go updateIndexFiles()
	}
	//}
}

func masterNodeWorker(port int) {
	fmt.Println("[INFO] Master http-server is starting...")
	r := mux.NewRouter() //.StrictSlash(true)
	//r.HandleFunc("/", HomeHandler)
	r.HandleFunc("/part", partUploadHandler).Methods("POST")
	r.HandleFunc("/parts", partsListHandler)
	//staticPartHandler := http.StripPrefix("/part/", http.FileServer(http.Dir(NodeConfig.DataDir)))
	///r.PathPrefix("/part").Handler(http.StripPrefix("/", http.FileServer(http.Dir(NodeConfig.DataDir)))).Methods("GET")
	//http.Handle("/", r)
	//r.HandleFunc("/part/", staticPartHandler).Methods("GET")
	//r.HandleFunc("/part/{id}", partDownloadHandler).Methods("GET")
	r.PathPrefix("/part/").Handler(http.StripPrefix("/part/",
		http.FileServer(http.Dir(NodeConfig.DataDir)))).Methods("GET")
	r.HandleFunc("/part/{id}", partDeleteHandler).Methods("DELETE")
	r.HandleFunc("/file", fileUploadHandler).Methods("POST")
	r.HandleFunc("/files", fileListHandler)
	r.HandleFunc("/file/{id}", fileDownloadHandler).Methods("GET")
	r.HandleFunc("/file/info", getFileInfoHandler).Methods("POST")
	r.HandleFunc("/file/info/{id}", fileInfoHandler).Methods("GET")
	r.HandleFunc("/file/{id}", fileDeleteHandler).Methods("DELETE")
	r.HandleFunc("/nodes", nodeListHandler)
	r.HandleFunc("/node/info", nodeInfoHandler)

	//r.HandleFunc("/articles", ArticlesHandler)
	//http.HandleFunc("/", fileDownloadHandler)
	//http.HandleFunc("/", fileUploadHandler)
	//http.ListenAndServe(":8086", nil)
	fmt.Printf("###########################\nAvailable methods on API:\n\n")
	r.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		t, err := route.GetPathTemplate()
		if err != nil {
			return err
		}
		fmt.Println("> " + t)
		return nil
	})
	fmt.Printf("###########################\n")

	srv := &http.Server{
		Handler: r,
		Addr:    ":" + strconv.Itoa(NodeConfig.NodePort),
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 5 * time.Minute,
		ReadTimeout:  5 * time.Minute,
	}
	//fmt.Println(srv.Addr)
	err := srv.ListenAndServe()
	if err != nil {
		HandleError(err)
	}

}

func fileNodeWorker(port int) {
	fmt.Println("[INFO] File http-server is starting...")
	http.ListenAndServe(":"+string(port), nil)
}

func jsonPrettyPrint(in string) string {
	var out bytes.Buffer
	err := json.Indent(&out, []byte(in), "", "\t")
	if err != nil {
		return in
	}
	return out.String()
}
func ProcessFile(filename string) *File {
	in, err := os.Open(workingDirectory + filename)
	if err != nil {
		panic(err)
		return nil
	}
	fstat, err := in.Stat()
	fileID, _ := shortid.Generate()
	var fSize = fstat.Size()
	var bytesLeft = fSize
	var f = &File{
		ID:   fileID,
		Name: filename,
		Size: fSize,
	}
	defer in.Close()
	var counter1 = 0
	for {
		var n = int64(chunkSize)
		if err != nil && err != io.EOF {
			panic(err)
		} else if err != nil && err == io.EOF {
			break
		}
		if bytesLeft < 0 {
			break
		}
		//outputName, _ := shortid.Generate()
		var choosenNode *Node = Nodes[rand.Intn(len(Nodes))]
		var replicaNode *Node
		for {
			replicaNode = Nodes[rand.Intn(len(Nodes))]
			if *replicaNode != *choosenNode {
				break
			}
		}
		//	var replicaNode *Node = getRandomExcept(*nodes, &choosenNode)
		var p Part
		/*{
			ID:            outputName,
			Size:          n,
			MainNodeID:    choosenNode.ID,
			ReplicaNodeID: replicaNode.ID,
			CreatedAt:     time.Now(),
		}*/
		//var writers []io.Writer
		//out, err := os.OpenFile("./testDir/data/"+outputName, os.O_CREATE|os.O_RDWR, 0666)
		in_r := io.LimitReader(in, int64(chunkSize))
		out, err := http.Post(fmt.Sprintf("http://%s/part", choosenNode.Address), "application/octet-stream", in_r)
		if err != nil {
			HandleError(err)
		}
		defer out.Body.Close()

		err = json.NewDecoder(out.Body).Decode(&p)
		if err != nil {
			HandleError(err)
		}

		/*if _, err = io.CopyN(out.Request.Body, in, n); err != nil && err != io.EOF {
			panic(err)
			return nil
		}*/
		bytesLeft -= int64(chunkSize)
		//	err = out.Body.Sync()
		/*		if err != nil {
				panic(err)
				return nil
			}*/
		f.AddPart(&p)
		Parts = append(Parts, &p)
		counter1++
		//choosenNode.FilesCount++
		//replicaNode.FilesCount++
		choosenNode.UsedSpace += int64(n)
		//replicaNode.UsedSpace += int64(n)
	}
	Files.Add(f)
	//	Files.List = append(Files.List, f)
	fmt.Printf("[INFO] Added new file %s, %s\n", f.ID, f.Name)
	updateIndexFiles()
	return f
}

func processFile(fileName string) File {

	fileID, _ := shortid.Generate()
	// open input file
	fi, err := os.Open(NodeConfig.DataDir + fileName)
	if err != nil {
		panic(err)
	}
	fstat, err := fi.Stat()
	if err != nil {
		log.Fatal(err)
	}
	var f = File{
		ID:   fileID,
		Name: fileName,
		Size: fstat.Size(),
	}
	// close fi on exit and check for its returned error
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()
	var counter1 = 0
	buf := make([]byte, chunkSize)
	for {
		r := bufio.NewReader(fi)
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}
		outputName, _ := shortid.Generate()
		var choosenNode *Node = Nodes[rand.Intn(len(Nodes))]
		var replicaNode *Node
		for {
			replicaNode = Nodes[rand.Intn(len(Nodes))]
			if *replicaNode != *choosenNode {
				break
			}
		}
		//	var replicaNode *Node = getRandomExcept(*nodes, &choosenNode)
		var p = Part{
			ID:            outputName,
			Size:          int64(n),
			MainNodeID:    choosenNode.ID,
			ReplicaNodeID: replicaNode.ID,
			CreatedAt:     time.Now(),
		}
		fo, err := os.Create(NodeConfig.DataDir + outputName)
		if err != nil {
			panic(err)
		}
		// close fo on exit and check for its returned error
		defer func() {
			if err := fo.Close(); err != nil {
				panic(err)
			}
		}()

		w := bufio.NewWriter(fo)

		if _, err := w.Write(buf[:n]); err != nil {
			panic(err)
		}
		if err = w.Flush(); err != nil {
			panic(err)
		}
		f.AddPart(&p)
		Parts = append(Parts, &p)
		counter1++
		//choosenNode.FilesCount++
		//replicaNode.FilesCount++
		choosenNode.UsedSpace += int64(n)
		//replicaNode.UsedSpace += int64(n)

	}
	Files.Add(&f)
	//Files.List = append(Files.List, &f)
	fmt.Printf("[INFO] Added new file %s, %s\n", f.ID, f.Name)
	updateIndexFiles()

	//fileJson, _ := json.Marshal(f)
	return f
}

func getFileName(str string) string {
	i, j := strings.LastIndex(str, "/"), strings.LastIndex(str, path.Ext(str))
	return str[i:j]
}

/*func getRandomExcept(nodes []Node, node *Node) *Node {
	for {
		replicaNode := *nodes[rand.Intn(len(*nodes))]
		if replicaNode == node {
			continue
		}
		return replicaNode

	}
}*/

func foreverWorker() {
	for {
		time.Sleep(1 * time.Second)
	}
}

func findFileByID(files *[]File, id string) *File {
	for _, v := range *files {
		if v.ID == id {
			return &v
			break
		}
	}
	return nil
}

func WriteDataToFile(filename string, data *[]byte) {
	f, err := os.OpenFile(NodeConfig.DataDir+"tmp_"+filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)

	if err != nil {
		panic(err)
	}
	n, err := f.WriteString(string(*data))
	if n != 0 && err != nil {
		panic(err)
	}
	f.Close()
}

func CopyData(src string, dst string) (err error) {
	in, err := os.Open(NodeConfig.DataDir + src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.OpenFile(NodeConfig.DataDir+dst, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

func consoleWorker() {
	time.Sleep(2 * time.Second)
	reader := bufio.NewReader(os.Stdin)
	for {
		var err error
		fmt.Print("#>: ")
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		switch text {
		case "":
			continue
		case "files":
			filesJson, _ := json.Marshal(Files.List)
			fmt.Println(jsonPrettyPrint(string(filesJson)))
		case "nodes":
			nodesJson, _ := json.Marshal(Nodes)
			fmt.Println(jsonPrettyPrint(string(nodesJson)))
			//fmt.Println("All nodes started")
		case "exit":
			fmt.Println("Exiting....")
			os.Exit(0)
		case "download":

			//testing find
		default:
			//fmt.Println("Error: no command found: " + text)
			//var f = Files.Find(text)
			//fileJson, _ := json.Marshal(f)
			//fmt.Println(jsonPrettyPrint(string(fileJson)))
			//	err = Files.Find(text).Download()
		}
		if err != nil {
			HandleError(err)
		}
	}
}

func loadExistingFile(filename string) ([]byte, error) {
	contents, err := ioutil.ReadFile(filename)
	return contents, err
}

func readIndexFile() error {
	var _filesJson, err = loadExistingFile(workingDirectory + "files.json")
	err = json.Unmarshal(_filesJson, &Files.List)
	if err != nil {
		HandleError(err)
	}
	(Nodes.GetCurrentNode()).FilesCount = len(Files.List)
	defer fmt.Printf("[INFO] Loaded %d files from index.\n", len(Files.List))
	return err
}

func getPartsCount() int {
	parts, _ := ioutil.ReadDir(NodeConfig.DataDir)
	return len(parts)
}

func loadNodesList() error {
	var _nodesJson, err = loadExistingFile(workingDirectory + "nodes.json")
	if err != nil {
		return err
	}
	err = json.Unmarshal(_nodesJson, &Nodes)
	defer fmt.Printf("[INFO] Loaded %d nodes from list.\n", len(Nodes))
	if len(Nodes) < 1 {
		defer fmt.Println("[ERR] No nodes loaded...")
	}
	return err
}

func IsEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1) // Or f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err // Either not empty or error, suits both cases
}

func findLessLoadedNode() *Node {

	var nodesListSorted NodeArr
	nodesListSorted = append(nodesListSorted, Nodes...)

	sort.Slice(nodesListSorted, func(i, j int) bool {
		return (*nodesListSorted[i]).UsedSpace < (*nodesListSorted[j]).UsedSpace

	})
	return nodesListSorted[0]
}

func findLessLoadedNodes(n int) NodeArr {
	var nodesListSorted NodeArr
	nodesListSorted = append(nodesListSorted, Nodes...)

	sort.Slice(nodesListSorted, func(i, j int) bool {
		return (nodesListSorted[i]).UsedSpace > (nodesListSorted[j]).UsedSpace
	})

	return nodesListSorted[n:]
}

func (n NodeArr) Save() {
	/*nodesJson, _ := json.Marshal(Nodes)
	ioutil.WriteFile(workingDirectory+"nodes.json", nodesJson, 0644)*/
}

func updateIndexFiles() {
	filesJson, _ := json.Marshal(Files.List)
	//partsJson, _ := json.Marshal(Parts)
	ioutil.WriteFile(workingDirectory+"files.json", filesJson, 0644)
	//ioutil.WriteFile(workingDirectory+"parts.json", partsJson, 0644)
}

func addNewNode(id string, name string, address string, totalSpace int64, usedSpace int64) Node {
	node := Node{id, name, address, true, 31457280, 0, 0, 0, time.Now()}
	Nodes = append(Nodes, &node)
	return node
}

func (fs FilesSlice) Find(value string) <-chan *File {
	fc := make(chan *File)
	f := func() {
		fs.Lock()
		defer fs.Unlock()
		for _, v := range fs.List {
			if v.ID == value {
				fc <- v
			}
		}
		close(fc)
	}
	go f()

	return fc
}

func (f FilesSlice) FindPart(value string) *Part {
	for _, v := range f.List {
		for _, z := range v.Parts {
			if z.ID == value {
				return z
			}
		}
	}
	return nil
}

/*
func (p *Part) GetData() error {

}*/

func (f *File) Download(w *http.ResponseWriter, ratio int64) (err error) {
	//var buf = make([]byte, f.Size)
	if f.IsAvailable() {
		//	var readers []io.Reader
		fmt.Printf("Downloading file \"%s\" - contains from %d parts\n", f.Name, len(f.Parts))
		partCounter := 0
		for _, v := range f.Parts {
			//fmt.Println(v)
			if v != nil && Nodes.FindNode(v.MainNodeID) != nil {
				// check if node is active first. if not - using replica server
				fmt.Printf("[MSINFO] Getting part #%d - %s from server %s...\n", partCounter, v.ID, Nodes.FindNode(v.MainNodeID).Address)
				node := Nodes.FindNode(v.MainNodeID)
				//temp_buf, err := v.GetData()
				if !node.IsActive {
					//fmt.Println("[WARN] MainNode is not available, trying to get data from replica nodes...")
					node = v.FindLiveReplica()
					if node == nil {
						err = errors.New(fmt.Sprintf("[ERR] No nodes available to download part %s, can't finish download.", v.ID))
						HandleError(err)
						return err
					}
				}
				partCounter++
				/*
					tr := &http.Transport{
						MaxIdleConns:       10,
						IdleConnTimeout:    30 * time.Second,
						DisableCompression: true,
					}
					client := &http.Client{Transport: tr}
					resp, err := client.Get("https://example.com")*/
				resp, err := http.Get(fmt.Sprintf("http://%s/part/%s", node.Address, v.ID))
				if err != nil {
					HandleError(err)
					return err
					//	break
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					err = errors.New(fmt.Sprintf("Node %s not have part %s", node.Address, v.ID))
					return err
				}
				fmt.Printf("[MSINFO] Streaming part #%d - %s to the client \n", partCounter, v.ID)
				//	readers = append(readers, resp.Body)
				for i := v.Size; i > 0; i -= ratio / 10 {
					if _, err = io.CopyN(*w, resp.Body, ratio/10); err != nil && err != io.EOF {
						HandleError(err)
						return err
					}
					time.Sleep(100 * time.Millisecond)
				}
				//time.Sleep(2 * time.Second)
				partCounter++
			} else {
				err = errors.New(fmt.Sprintf("[ERR] No nodes available to download part %s, can't finish download.", v))
				HandleError(err)
				return err
			}

		}
		/*multiR := io.MultiReader(readers...)
		if err != nil {
			HandleError(err)
			return err
		}*/

	} else {
		err = errors.New(fmt.Sprintf("[ERR] Not all nodes available to download file %s", f.ID))
		return err
	}
	return err
}

func DownloadPart(w *http.ResponseWriter, id string) (err error) {
	//var buf = make([]byte, f.Size)

	//var readers []io.Reader

	in, err := os.Open(NodeConfig.DataDir + id)
	fstat, err := in.Stat()
	var fSize = fstat.Size()
	fmt.Printf("[PSINFO] Sending part \"%s\", size = %d KByte(s)...\n", id, fSize/1024)
	if err != nil {
		HandleError(err)
		//panic(err)
	}
	defer in.Close()
	fr := bufio.NewReader(in)

	if _, err = io.Copy(*w, fr); err != nil {
		HandleError(err)
	}
	return err
}

func (n NodeArr) FindNode(value string) *Node {
	for _, v := range n {
		//fmt.Println(v)
		//fmt.Println(value)
		if v.ID == value || v.Address == value {
			return v
		}
	}
	return nil
}

func (p *Part) IsAnyReplicaAlive() bool {
	for _, v := range p.ReplicaNodesID {
		if Nodes.FindNode(v) != nil && Nodes.FindNode(v).IsActive {
			return true
		}

	}
	return false
}

func (p *Part) FindLiveReplica() *Node {
	for _, v := range p.ReplicaNodesID {
		n := Nodes.FindNode(v)
		if n.IsActive {
			return n
		}

	}
	return nil
}

func (f *File) IsAvailable() bool {
	for _, z := range f.Parts {
		if (Nodes.FindNode(z.MainNodeID) == nil || !(Nodes.FindNode(z.MainNodeID)).IsActive) && !z.IsAnyReplicaAlive() {
			return false
		}
	}
	return true
}

/*
func downloadHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	StoredAs := r.Form.Get("StoredAs") // file name
	data, err := ioutil.ReadFile("files/" + StoredAs)
	if err != nil {
		fmt.Fprint(w, err)
	}
	http.ServeContent(w, r, StoredAs, time.Now(), bytes.NewReader(data))
}*/
