package main

import (
	"bufio"
	"bytes"
	//"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	//"github.com/satori/go.uuid"
	"github.com/ventu-io/go-shortid"
	"io"
	//"net/http"
	"math/rand"
	"os"
	"path"
	"strings"
	"time"
)

type Node struct {
	Id      int    `json:"Id"`
	Name    string `json:"Name,omitempty"`
	Address string `json:"Address,omitempty"`
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

func main() {

	var Files = []File{}
	// defining port input flags

	httpPort := flag.Int("port", 8080, "an int")
	clusterNodes := flag.Args() // all other (defines like: "node1:81 node2:81 node3:81")
	flag.Parse()

	var nodes = []Node{}
	node1 := Node{1, "node1", "127.0.0.1:8081"}
	node2 := Node{2, "node2", "127.0.0.1:8082"}
	node3 := Node{3, "node3", "127.0.0.1:8083"}
	nodes = append(nodes, node1)
	nodes = append(nodes, node2)
	nodes = append(nodes, node3)
	nodesJson, _ := json.Marshal(nodes)

	fmt.Printf("Hello, world.\n")
	fmt.Println("Port:", *httpPort)
	fmt.Println("Nodes:", clusterNodes)
	fmt.Println(jsonPrettyPrint(string(nodesJson)))
	//http.HandleFunc("/upload", uploadHandler)
	//http.ListenAndServe(":8080", nil)
	var f1, f2 File
	go func() {
		f1 = processFile("1.dmg", nodes)
		Files = append(Files, f1)
	}()

	go func() {
		f2 = processFile("1.mov", nodes)
		Files = append(Files, f2)
	}()
	time.Sleep(5 * time.Second)
	filesJson, _ := json.Marshal(Files)
	fmt.Println(jsonPrettyPrint(string(filesJson)))
	foreverWorker()

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
	var chunkSize = 31457280 // 10 megabytes

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
		choosenNode := nodes[rand.Intn(len(nodes))]
		replicaNode := getRandomExcept(nodes, choosenNode)
		var p = Part{
			Id:          counter1,
			Name:        outputName,
			MainNode:    &choosenNode,
			ReplicaNode: &replicaNode,
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

	//fileJson, _ := json.Marshal(f)
	return f
}

func getFileName(str string) string {
	i, j := strings.LastIndex(str, "/"), strings.LastIndex(str, path.Ext(str))
	return str[i:j]
}

func getRandomExcept(nodes []Node, node Node) Node {
	for {
		replicaNode := nodes[rand.Intn(len(nodes))]
		if replicaNode == node {
			continue
		}
		return replicaNode

	}
}

func foreverWorker() {
	for {
		time.Sleep(1 * time.Second)
	}
}
