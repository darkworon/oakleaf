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
	//"runtime"
	//"runtime/pprof"
	"strconv"
	"strings"
	"time"
)

type Node struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Address    string    `json:"address"`
	IsActive   bool      `json:"is_active"`
	TotalSpace int64     `json:"total_space"`
	UsedSpace  int64     `json:"used_space"`
	FilesCount int       `json:"files_count"`
	LastUpdate time.Time `json:"last_update"`
	//Parts      []string  `json:"parts,omitempty"`
}

type Part struct {
	ID            string    `json:"id"`
	Size          int64     `json:"size"`
	CreatedAt     time.Time `json:"created_at"`
	MainNodeID    string    `json:"main_nodeID"`
	ReplicaNodeID string    `json:"replica_nodeID"`
}

type File struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
	//Parts []*Part `json:"Parts"`
	Parts []string `json:"parts,omitempty"`
}

type PublicFile struct {
	*File
	Parts omit `json:"parts,omitempty"`
}

type FileArr []*File
type PartArr []*Part
type NodeArr []*Node
type omit *struct{}

func (file *File) AddPart(part *Part) *File {
	file.Parts = append(file.Parts, part.ID)
	return file
}

var Files = FileArr{}
var Parts = PartArr{}
var Nodes = NodeArr{}

const (
	chunkSize         = 157286400
	defaultWorkingDir = "testDir"
	defaultPort       = 8086
)

var (
	workingDirectory     string
	dataStorageDirectory string
	nodePort             int
)

var memprofile = "./oakleaf1.mprof"

func configWorker() {

}

func init() {

	flag.StringVar(&workingDirectory, "dir", defaultWorkingDir, "working directory")
	flag.IntVar(&nodePort, "port", defaultPort, "node server port")
	flag.Parse()
	if workingDirectory[:1] != "/" {
		workingDirectory += "/"
	}
	dataStorageDirectory = workingDirectory + "data/"
	fmt.Println("Working directory: " + workingDirectory)
	fmt.Println("Data storage directory: " + dataStorageDirectory)
	fmt.Println("Node port: " + strconv.Itoa(nodePort))

	os.MkdirAll(dataStorageDirectory, os.ModePerm)

	fmt.Println("[INFO] Oakleaf server node is initializing...")
	_ = loadNodesList()
	//time.Sleep(1 * time.Second)

	go nodesInfoWorker()
	go func() {
		masterNodeWorker(nodePort)
	}()
	_ = readIndexFile()
}

func main() {
	//go fileNodeWorker(3801)
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
	filesJson, _ := json.Marshal(Files)
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
		var f = Files.Find(vars["id"])
		//w.Header().Set("Content-Length", string(f.Size))
		if f == nil {
			// not found file on this node - trying to get info from other nodes
			f = getFileInfoFromAllNodes(vars["id"])
		}
		if f.ID != "" {
			w.Header().Set("Content-Disposition", "attachment; filename="+f.Name)
			w.WriteHeader(http.StatusOK)
			err := f.Download(&w)
			if err != nil {
				errorHandler(w, r, 500)
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
			fmt.Println(err)
			continue
		}
		defer resp.Body.Close()
		err = json.NewDecoder(resp.Body).Decode(&f)
		if err != nil {
			// if cant unmarshall - thinking we got no file info
			//fmt.Println(err)
			continue
		}
		if f != nil {
			//fmt.Println(f)
			fmt.Println("Found it! Adding to our list and sending back")
			Files = append(Files, f)
			go updateIndexFiles()
			break
		}
	}
	return f
}

func fileInfoHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	var f = Files.Find(vars["id"])
	if f != nil {
		fileJson, _ := json.Marshal(f)
		w.WriteHeader(http.StatusOK)
		w.Write(fileJson)
	} else {
		errorHandler(w, r, 404)
	}
}

func partUploadHandler(w http.ResponseWriter, r *http.Request) {
	file, _, err := r.FormFile("data") // img is the key of the form-data
	defer file.Close()

	//	fmt.Println(handler.Filename)
	if err != nil {
		fmt.Println(err)
	}
	var size int64 = 0
	//	fmt.Println(r.Header)
	//	r.Body = http.MaxBytesReader(w, r.Body, math.MaxInt64)
	name, _ := shortid.Generate()
	out, _ := os.OpenFile(dataStorageDirectory+name, os.O_CREATE|os.O_WRONLY, 0666)
	defer out.Close()

	if err != nil {
		fmt.Println(err)
	}
	size, err = io.Copy(out, file)
	if err != nil && err != io.EOF {
		fmt.Println(err)
	}

	var p = &Part{
		ID:            name,
		Size:          size,
		MainNodeID:    Nodes[0].ID,
		ReplicaNodeID: Nodes[1].ID,
		CreatedAt:     time.Now(),
	}
	partJson, _ := json.Marshal(p)
	Parts = append(Parts, p)
	w.Write([]byte(jsonPrettyPrint(string(partJson))))
	updateIndexFiles()

}

func fileUploadHandler(w http.ResponseWriter, r *http.Request) {
	//r.Body = http.MaxBytesReader(w, r.Body, math.MaxInt64)
	file, handler, err := r.FormFile("data") // img is the key of the form-data
	//fmt.Println(r.ContentLength)

	if err != nil {
		fmt.Println(err)
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
		pr, pw := io.Pipe()
		mpw := multipart.NewWriter(pw)

		in_r := io.LimitReader(file, int64(chunkSize))
		var choosenNode *Node = Nodes[rand.Intn(len(Nodes))]
		var replicaNode *Node
		for {
			replicaNode = Nodes[rand.Intn(len(Nodes))]
			if *replicaNode != *choosenNode {
				break
			}
		}
		var size int64 = 0
		go func() {
			var part io.Writer
			defer pw.Close()

			if part, err = mpw.CreateFormFile("data", handler.Filename); err != nil && err != io.EOF {
				log.Fatal(err)
			}
			if size, err = io.Copy(part, in_r); err != nil && err != io.EOF {
				panic(err)

			}
			if err = mpw.Close(); err != nil {
				log.Fatal(err)
			}
		}()
		resp, err := http.Post(fmt.Sprintf("http://%s/part", choosenNode.Address), mpw.FormDataContentType(), pr)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()

		err = json.NewDecoder(resp.Body).Decode(&p)
		if err != nil {
			fmt.Println(err)
		}

		f.AddPart(&p)
		Parts = append(Parts, &p)
		//partJson, _ := json.Marshal(p)
		//fmt.Println(partJson)
		choosenNode.FilesCount++
		replicaNode.FilesCount++
		choosenNode.UsedSpace += p.Size
		replicaNode.UsedSpace += p.Size
		fileSizeCounter += p.Size
		if p.Size < int64(chunkSize) {
			break
		}
	}

	//	f.Size = fileSizeCounter
	f.Size = fileSizeCounter
	Files = append(Files, f)
	fileJson, _ := json.Marshal(PublicFile{
		File: f,
	})
	fmt.Printf("[INFO] Added new file %s, %s\n", f.ID, f.Name)
	updateIndexFiles()
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

}

func partDownloadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if vars["id"] != "" {
		var p = Parts.Find(vars["id"])
		//w.Header().Set("Content-Length", string(f.Size))
		if p != nil {
			//	w.Header().Set("Content-Type", "application/octet-stream")
			//	w.Header().Set("Content-Length", string(f.Size))
			w.Header().Set("Content-Disposition", "attachment; filename="+p.ID)
			w.WriteHeader(http.StatusOK)
			err := p.Download(&w)
			if err != nil {
				errorHandler(w, r, 500)
			}
		} else {
			fmt.Fprintf(w, "Not found part with id %s", vars["id"])
			fmt.Printf("[PSINFO] 404 - not found part \"%s\"\n", vars["id"])
		}
	}

}

func partDeleteHandler(w http.ResponseWriter, r *http.Request) {
}

func fileDeleteHandler(w http.ResponseWriter, r *http.Request) {
}

func masterNodeWorker(port int) {
	fmt.Println("[INFO] Master http-server is starting...")
	r := mux.NewRouter()
	//r.HandleFunc("/", HomeHandler)
	r.HandleFunc("/part", partUploadHandler).Methods("POST")
	r.HandleFunc("/parts", partsListHandler)
	r.HandleFunc("/part/{id}", partDownloadHandler).Methods("GET")
	r.HandleFunc("/part/{id}", partDeleteHandler).Methods("DELETE")
	r.HandleFunc("/file", fileUploadHandler).Methods("POST")
	r.HandleFunc("/files", fileListHandler)
	r.HandleFunc("/file/{id}", fileDownloadHandler).Methods("GET")
	r.HandleFunc("/file/info/{id}", fileInfoHandler).Methods("GET")
	r.HandleFunc("/file/{id}", fileDeleteHandler).Methods("DELETE")

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
		Addr:    ":" + strconv.Itoa(nodePort),
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 30 * time.Second,
		ReadTimeout:  30 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())

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
			fmt.Println(err)
		}
		defer out.Body.Close()

		err = json.NewDecoder(out.Body).Decode(&p)
		if err != nil {
			fmt.Println(err)
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
		choosenNode.FilesCount++
		replicaNode.FilesCount++
		choosenNode.UsedSpace += int64(n)
		replicaNode.UsedSpace += int64(n)
	}
	Files = append(Files, f)
	fmt.Printf("[INFO] Added new file %s, %s\n", f.ID, f.Name)
	updateIndexFiles()
	return f
}

func processFile(fileName string) File {

	fileID, _ := shortid.Generate()
	// open input file
	fi, err := os.Open(dataStorageDirectory + fileName)
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
		fo, err := os.Create(dataStorageDirectory + outputName)
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
		choosenNode.FilesCount++
		replicaNode.FilesCount++
		choosenNode.UsedSpace += int64(n)
		replicaNode.UsedSpace += int64(n)

	}
	Files = append(Files, &f)
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
	f, err := os.OpenFile(dataStorageDirectory+"tmp_"+filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
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
	in, err := os.Open(dataStorageDirectory + src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.OpenFile(dataStorageDirectory+dst, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
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
			//	err = Files.Find(text).Download()
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

func readIndexFile() (err error) {
	var _filesJson = loadExistingFile(workingDirectory + "files.json")
	err = json.Unmarshal(_filesJson, &Files)
	fmt.Printf("[INFO] Loaded %d files from index.\n", len(Files))

	var _partsJson = loadExistingFile(workingDirectory + "parts.json")
	err = json.Unmarshal(_partsJson, &Parts)
	fmt.Printf("[INFO] Loaded %d parts from index.\n", len(Parts))

	if len(Files) < 1 {
		//go addTestFiles()
	}
	return err
}

func loadNodesList() error {
	var _nodesJson = loadExistingFile(workingDirectory + "nodes.json")

	err := json.Unmarshal(_nodesJson, &Nodes)
	fmt.Printf("[INFO] Loaded %d nodes from list.\n", len(Nodes))
	if len(Nodes) < 1 {
		n1_id, _ := shortid.Generate()
		node1 := Node{n1_id, "node1", "127.0.0.1:8086", true, 32212254720, 0, 0, time.Now()}
		n2_id, _ := shortid.Generate()
		node2 := Node{n2_id, "node2", "127.0.0.1:8087", true, 32212254720, 0, 0, time.Now()}
		Nodes = append(Nodes, &node1)
		Nodes = append(Nodes, &node2)
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
		ioutil.WriteFile(workingDirectory+"nodes.json", nodesJson, 0644)
		time.Sleep(1 * time.Second)
	}
}

func updateIndexFiles() {
	filesJson, _ := json.Marshal(Files)
	partsJson, _ := json.Marshal(Parts)
	ioutil.WriteFile(workingDirectory+"files.json", filesJson, 0644)
	ioutil.WriteFile(workingDirectory+"parts.json", partsJson, 0644)
}

func addNewNode(id string, name string, address string, totalSpace int64, usedSpace int64) Node {
	node := Node{id, name, address, true, 31457280, 0, 0, time.Now()}
	Nodes = append(Nodes, &node)
	return node
}

func (f FileArr) Find(value string) *File {
	for _, v := range f {
		if v.ID == value {
			return v
		}
	}
	return nil
}

func (f PartArr) Find(value string) *Part {
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

func (f *File) Download(w *http.ResponseWriter) (err error) {
	//var buf = make([]byte, f.Size)

	var readers []io.Reader
	fmt.Printf("Downloading file \"%s\" - contains from %d parts\n", f.Name, len(f.Parts))
	partCounter := 0
	for _, v := range f.Parts {
		var p = Parts.Find(v)
		if p != nil {
			// check if node is active first. if not - using replica server
			fmt.Printf("[MSINFO] Getting part #%d - %s from server %s...\n", partCounter, v, Nodes.FindNode(p.MainNodeID).Address)
			node := Nodes.FindNode(p.MainNodeID)
			//temp_buf, err := v.GetData()
			if node.IsActive {
				//err = CopyData(v.Name, "tmp_"+f.Name)
				//in, err := os.Open("./testDir/data/" + v)
				//if err != nil {
				//	panic(err)
				//}
				//defer in.Close()
				//r := bufio.NewReader(in)
				//r, err := http.NewRequest("GET", fmt.Sprintf("http://%s/part/%s", node.Address, v.Name), nil)
				resp, err := http.Get(fmt.Sprintf("http://%s/part/%s", node.Address, p.ID))
				if err != nil {
					fmt.Println(err)
					break
				}
				fmt.Printf("[MSINFO] Streaming part #%d - %s to the client \n", partCounter, p.ID)
				defer resp.Body.Close()
				readers = append(readers, resp.Body)
				partCounter++
			} else if p.ReplicaNodeID != "" {
				fmt.Println("[WARN] MainNode is not available, trying to get data from ReplicaNode...")
				if Nodes.FindNode(p.ReplicaNodeID).IsActive {
					//	err = CopyData(v.Name, f.Name+".tmp")
				} else {
					err = errors.New(fmt.Sprintf("[ERR] No nodes available to download part %s, can't finish download.", p.ID))
					return err
				}
				partCounter++
			}

			if err != nil {
				fmt.Println(err)
			}
			multiR := io.MultiReader(readers...)
			if err != nil {
				fmt.Println(err)
			}
			if _, err = io.Copy(*w, multiR); err != nil {
				fmt.Println(err)
			}
		} else {
			err = errors.New(fmt.Sprintf("[ERR] No nodes available to download part %s, can't finish download.", v))
			fmt.Println(err)
			break
		}

	}
	return err
}

func (p *Part) Download(w *http.ResponseWriter) (err error) {
	//var buf = make([]byte, f.Size)

	//var readers []io.Reader
	fmt.Printf("[PSINFO] Sending part \"%s\", size = %d KByte(s)...\n", p.ID, p.Size/1024)
	in, err := os.Open("./testDir/data/" + p.ID)
	if err != nil {
		//panic(err)
	}
	defer in.Close()
	fr := bufio.NewReader(in)

	if _, err = io.Copy(*w, fr); err != nil {
		fmt.Println(err)
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
