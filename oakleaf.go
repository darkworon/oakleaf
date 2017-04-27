package main

import (
	"bufio"
	"bytes"
	//"encoding/base64"
	"encoding/json"
	//"flag"
	"fmt"
	"log"
	//"github.com/satori/go.uuid"
	"github.com/ventu-io/go-shortid"
	"io"
	//"net/http"
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	//"runtime"
	//	"runtime/pprof"
	"strings"
	"time"
)

type Node struct {
	ID         string    `json:"ID"`
	Name       string    `json:"Name,omitempty"`
	Address    string    `json:"Address,omitempty"`
	IsActive   bool      `json:"IsActive"`
	TotalSpace int64     `json:"TotalSpace"`
	UsedSpace  int64     `json:"UsedSpace"`
	FilesCount int       `json:"FilesCount"`
	LastUpdate time.Time `json:"LastUpdate"`
}

type Part struct {
	ID            int       `json:"ID"`
	Name          string    `json:"Name,omitempty"`
	Size          int64     `json:"Size"`
	CreatedAt     time.Time `json:"CreatedAt"`
	MainNodeID    string    `json:"MainNodeID"`
	ReplicaNodeID string    `json:"ReplicaNodeID"`
}

type File struct {
	ID    string  `json:"ID"`
	Name  string  `json:"Name"`
	Size  int64   `json:"Size"`
	Parts []*Part `json:"Parts"`
}

type FileArr []*File
type NodeArr []*Node

func (file *File) AddPart(part *Part) []*Part {
	file.Parts = append(file.Parts, part)
	return file.Parts
}

var Files = FileArr{}
var Nodes = NodeArr{}

var chunkSize = 52428800 // 50 megabytes
var memprofile = "./oakleaf1.mprof"

func main() {
	fmt.Println("[INFO] Oakleaf server node is starting...")
	_ = loadNodesList()
	_ = readIndexFile()
	// defining port input flags

	//httpPort := flag.Int("port", 8080, "an int")
	//clusterNodes := flag.Args() // all other (defines like: "node1:81 node2:81 node3:81")
	//flag.Parse()

	/*n1_id, _ := shortid.Generate()
	node1 := Node{n1_id, "node1", "127.0.0.1:8081", true, 32212254720, 0, 0, time.Now()}
	n2_id, _ := shortid.Generate()
	node2 := Node{n2_id, "node2", "127.0.0.1:8082", true, 32212254720, 0, 0, time.Now()}
	n3_id, _ := shortid.Generate()
	node3 := Node{n3_id, "node3", "127.0.0.1:8083", true, 32212254720, 0, 0, time.Now()}
	Nodes = append(Nodes, &node1)
	Nodes = append(Nodes, &node2)
	Nodes = append(Nodes, &node3)
	//nodesJson, _ := json.Marshal(Nodes)
	*/

	//fmt.Printf("Hello, world.\n")
	//fmt.Println("Port:", *httpPort)
	//fmt.Println("Nodes:", clusterNodes)
	//fmt.Println(jsonPrettyPrint(string(nodesJson)))
	//http.HandleFunc("/upload", uploadHandler)
	//http.ListenAndServe(":8080", nil)
	go func() {
		//processFile("1.dmg")
	}()
	go func() {
		//processFile("1.mov")
	}()
	//	time.Sleep(5 * time.Second)
	//	nodes[1].IsActive = false
	//	time.Sleep(2 * time.Second)
	//filesJson, _ := json.Marshal(Files)
	//fmt.Println(jsonPrettyPrint(string(filesJson)))
	//time.Sleep(10000)
	//var aa = findFileByID(Files, Files[1].ID)
	//fmt.Println(aa.Name)
	//foreverWorker()
	go nodesInfoWorker()
	consoleWorker()

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
	in, err := os.Open("./testDir/" + filename)
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
			ID:            counter1,
			Name:          outputName,
			Size:          n,
			MainNodeID:    choosenNode.ID,
			ReplicaNodeID: replicaNode.ID,
			CreatedAt:     time.Now(),
		}
		out, err := os.OpenFile("./testDir/"+outputName, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
			return nil
		}
		defer func() {
			cerr := out.Close()
			if err == nil {
				err = cerr
			}
		}()
		if _, err = io.CopyN(out, in, n); err != nil && err != io.EOF {
			panic(err)
			return nil
		}
		bytesLeft -= int64(chunkSize)
		err = out.Sync()
		if err != nil {
			panic(err)
			return nil
		}
		f.AddPart(&p)
		counter1++
		choosenNode.FilesCount++
		replicaNode.FilesCount++
		choosenNode.UsedSpace += int64(n)
		replicaNode.UsedSpace += int64(n)
	}
	Files = append(Files, f)
	fmt.Printf("[INFO] Added new file %s, %s\n", f.ID, f.Name)
	updateIndexFile()
	return f
}

func processFile(fileName string) File {

	fileID, _ := shortid.Generate()
	// open input file
	fi, err := os.Open("./testDir/" + fileName)
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
			ID:            counter1,
			Name:          outputName,
			Size:          int64(n),
			MainNodeID:    choosenNode.ID,
			ReplicaNodeID: replicaNode.ID,
			CreatedAt:     time.Now(),
		}
		fo, err := os.Create("./testDir/" + outputName)
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
		counter1++
		choosenNode.FilesCount++
		replicaNode.FilesCount++
		choosenNode.UsedSpace += int64(n)
		replicaNode.UsedSpace += int64(n)

	}
	Files = append(Files, &f)
	fmt.Printf("[INFO] Added new file %s, %s\n", f.ID, f.Name)
	updateIndexFile()

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
	f, err := os.OpenFile("./testDir/tmp_"+filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
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
	in, err := os.Open("./testDir/" + src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.OpenFile("./testDir/"+dst, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
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
			filesJson, _ := json.Marshal(Files)
			fmt.Println(jsonPrettyPrint(string(filesJson)))
		case "nodes":
			nodesJson, _ := json.Marshal(Nodes)
			fmt.Println(jsonPrettyPrint(string(nodesJson)))
		case "stop":
			Nodes[0].IsActive = false
			Nodes[1].IsActive = false
			Nodes[2].IsActive = false
			fmt.Println("All nodes stopped")
		case "start":
			Nodes[0].IsActive = true
			Nodes[1].IsActive = true
			Nodes[2].IsActive = true
			fmt.Println("All nodes started")
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
			err = Files.Find(text).Download()
		}
		if err != nil {
			fmt.Println(err)
		}
	}
}

func loadExistingFile(filename string) []byte {
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		//error
	}

	return contents
}

func readIndexFile() error {
	var _filesJson = loadExistingFile("./testDir/index.json")

	err := json.Unmarshal(_filesJson, &Files)
	fmt.Printf("[INFO] Loaded %d files from index.\n", len(Files))
	if len(Files) < 1 {
		go func() {
			ProcessFile("1.dmg")
		}()
		go func() {
			ProcessFile("1.mov")
		}()
	}
	return err
}

func loadNodesList() error {
	var _nodesJson = loadExistingFile("./testDir/nodes.json")

	err := json.Unmarshal(_nodesJson, &Nodes)
	fmt.Printf("[INFO] Loaded %d nodes from list.\n", len(Nodes))
	if len(Nodes) < 1 {
		n1_id, _ := shortid.Generate()
		node1 := Node{n1_id, "node1", "127.0.0.1:8081", true, 32212254720, 0, 0, time.Now()}
		n2_id, _ := shortid.Generate()
		node2 := Node{n2_id, "node2", "127.0.0.1:8082", true, 32212254720, 0, 0, time.Now()}
		n3_id, _ := shortid.Generate()
		node3 := Node{n3_id, "node3", "127.0.0.1:8083", true, 32212254720, 0, 0, time.Now()}
		Nodes = append(Nodes, &node1)
		Nodes = append(Nodes, &node2)
		Nodes = append(Nodes, &node3)
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

func getLessLoadedNode() *Node {
	var minFilesCount = 0
	var selectedNode *Node
	for _, v := range Nodes {
		if v.FilesCount > minFilesCount {
			minFilesCount = v.FilesCount
			selectedNode = v
		}
	}
	return selectedNode
}

func nodesInfoWorker() {
	for {
		nodesJson, _ := json.Marshal(Nodes)
		ioutil.WriteFile("./testDir/nodes.json", nodesJson, 0644)
		time.Sleep(1 * time.Second)
	}
}

func updateIndexFile() {
	filesJson, _ := json.Marshal(Files)
	ioutil.WriteFile("./testDir/index.json", filesJson, 0644)
}

func addNewNode(id string, name string, address string, totalSpace int64, usedSpace int64) Node {
	node := Node{id, name, address, true, 31457280, 0, 0, time.Now()}
	Nodes = append(Nodes, &node)
	return node
}

/*func Find(inArray []Model, value string) Model {
	for _, v := range inArray {
		if v.(struct{ ID string }).ID == value {
			return v
		}
	}
	return nil
}*/

func (f FileArr) Find(value string) *File {
	for _, v := range f {
		if v.ID == value {
			return v
		}
	}
	return nil
}

/*
func (p *Part) GetData() error {

}*/

func (f *File) Download() (err error) {
	//var buf = make([]byte, f.Size)
	fmt.Printf("Downloading file \"%s\" - contains from %d parts\n", f.Name, len(f.Parts))
	for _, v := range f.Parts {
		// check if node is active first. if not - using replica server
		fmt.Printf("[%d] Downloading part %s from server %s...\n", v.ID, v.Name, Nodes.FindNode(v.MainNodeID).Address)
		//temp_buf, err := v.GetData()
		if Nodes.FindNode(v.MainNodeID).IsActive {
			err = CopyData(v.Name, "tmp_"+f.Name)
		} else if v.ReplicaNodeID != "" {
			fmt.Println("[WARN] MainNode is not available, trying to get data from ReplicaNode...")
			if Nodes.FindNode(v.ReplicaNodeID).IsActive {
				err = CopyData(v.Name, f.Name+".tmp")
			} else {
				err = errors.New(fmt.Sprintf("[ERR] No nodes available to download part %s, can't finish download.", v.Name))
				return err
			}
		}

		if err != nil {
			fmt.Println(err)
		}

	}
	return err
}

func (n NodeArr) FindNode(value string) *Node {
	for _, v := range n {
		//fmt.Println(v)
		//fmt.Println(value)
		if v.ID == value {

			return v
		}
	}
	return nil
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
