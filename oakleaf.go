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
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strings"
	"time"
)

type Node struct {
	Id         string    `json:"Id"`
	Name       string    `json:"Name,omitempty"`
	Address    string    `json:"Address,omitempty"`
	IsActive   bool      `json:"IsActive"`
	LastUpdate time.Time `json:"LastUpdate"`
}

type Part struct {
	Id          int       `json:"Id"`
	Name        string    `json:"Name,omitempty"`
	CreatedAt   time.Time `json:"CreatedAt"`
	MainNode    *Node
	ReplicaNode *Node
}

type File struct {
	Id    string `json:"Id"`
	Name  string `json:"Name"`
	Size  int64  `json:"Size"`
	Parts []Part `json:"Parts"`
}

func (file *File) AddPart(part Part) []Part {
	file.Parts = append(file.Parts, part)
	return file.Parts
}

var Files = []File{}
var Nodes = []Node{}

func main() {
	fmt.Println("[INFO] Oakleaf server node is starting...")
	_ = readIndexFile()
	// defining port input flags

	//httpPort := flag.Int("port", 8080, "an int")
	//clusterNodes := flag.Args() // all other (defines like: "node1:81 node2:81 node3:81")
	//flag.Parse()

	n1_id, _ := shortid.Generate()
	node1 := Node{n1_id, "node1", "127.0.0.1:8081", true, time.Now()}
	n2_id, _ := shortid.Generate()
	node2 := Node{n2_id, "node2", "127.0.0.1:8082", true, time.Now()}
	n3_id, _ := shortid.Generate()
	node3 := Node{n3_id, "node3", "127.0.0.1:8083", true, time.Now()}
	Nodes = append(Nodes, node1)
	Nodes = append(Nodes, node2)
	Nodes = append(Nodes, node3)
	//nodesJson, _ := json.Marshal(Nodes)

	//fmt.Printf("Hello, world.\n")
	//fmt.Println("Port:", *httpPort)
	//fmt.Println("Nodes:", clusterNodes)
	//fmt.Println(jsonPrettyPrint(string(nodesJson)))
	//http.HandleFunc("/upload", uploadHandler)
	//http.ListenAndServe(":8080", nil)
	go func() {
		//	processFile("1.dmg", Nodes)
	}()
	go func() {
		//	processFile("1.mov", Nodes)
	}()
	//	time.Sleep(5 * time.Second)
	//	nodes[1].IsActive = false
	//	time.Sleep(2 * time.Second)
	//filesJson, _ := json.Marshal(Files)
	//fmt.Println(jsonPrettyPrint(string(filesJson)))
	//time.Sleep(10000)
	//var aa = findFileById(Files, Files[1].Id)
	//fmt.Println(aa.Name)
	//foreverWorker()
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

func processFile(fileName string, nodes []Node) File {
	var chunkSize = 104857600 // 100 megabytes

	fileId, _ := shortid.Generate()
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
		Id:   fileId,
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
		var choosenNode *Node = &nodes[rand.Intn(len(nodes))]
		var replicaNode *Node
		for {
			replicaNode = &nodes[rand.Intn(len(nodes))]
			if *replicaNode != *choosenNode {
				break
			}
		}
		//	var replicaNode *Node = getRandomExcept(*nodes, &choosenNode)
		var p = Part{
			Id:          counter1,
			Name:        outputName,
			MainNode:    choosenNode,
			ReplicaNode: replicaNode,
			CreatedAt:   time.Now(),
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
		f.AddPart(p)
		counter1++
	}
	Files = append(Files, f)
	fmt.Printf("[INFO] Added new file %s, %s\n", f.Id, f.Name)
	filesJson, _ := json.Marshal(Files)
	ioutil.WriteFile("./testDir/index.json", filesJson, 0644)

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

func findFileById(files *[]File, id string) *File {
	for _, v := range *files {
		if v.Id == id {
			return &v
			break
		}
	}
	return nil
}

func consoleWorker() {
	reader := bufio.NewReader(os.Stdin)
	for {
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
			Nodes[1].IsActive = false
			fmt.Println("Node2 stopped")
		case "start":
			Nodes[1].IsActive = true
			fmt.Println("Node2 started")

		case "exit":
			fmt.Println("Exiting....")
			os.Exit(0)
		default:
			fmt.Println("Error: no command found: " + text)
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
