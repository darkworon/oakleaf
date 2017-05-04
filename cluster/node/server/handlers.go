package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	//"oakleaf/cluster"
	"fmt"
	"github.com/google/go-querystring/query"
	"github.com/ventu-io/go-shortid"
	"io"
	"mime/multipart"
	"net/url"
	"oakleaf/cluster"
	"oakleaf/config"
	"oakleaf/file"
	"oakleaf/part"
	//"oakleaf/node/client"
	"bufio"
	"oakleaf/cluster/node"
	"oakleaf/storage"
	"oakleaf/utils"
	"os"
	"path/filepath"
	"time"
)

var conf = config.NodeConfig
var nodes = cluster.Nodes
var files = storage.Files

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

	w.Write([]byte(utils.JsonPrettyPrint(string(storage.Files.ToJson()))))
}

func fileDownloadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if vars["id"] != "" {
		var f = <-storage.Files.Find(vars["id"])
		//w.Header().Set("Content-Length", string(f.Size))
		if f == nil {
			// not found file on this node - trying to get info from other nodes
			//File.FromJson(cluster.FindFile(vars["id"]), conf)
			f2 := file.File{}
			nodes.FindFile(conf, vars["id"], &f2)
		}
		if f.ID != "" {
			if f.IsAvailable() {
				setDownloadHeaders(w, f)
				w.WriteHeader(http.StatusOK)
				err := f.Download(&w, conf.DownlinkRatio)
				if err != nil {
					utils.HandleError(err)
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

func setDownloadHeaders(w http.ResponseWriter, f *file.File) {
	w.Header().Set("Content-Description", "File Transfer")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename="+f.Name)
	w.Header().Set("Content-Transfer-Encoding", "binary")
	w.Header().Set("Expires", "0")
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Pragma", "public")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", f.Size))
}

func fileInfoHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	var f = <-storage.Files.Find(vars["id"])
	if f != nil {
		w.WriteHeader(http.StatusOK)
		w.Write(f.ToJson())
	} else {
		errorHandler(w, r, 404)
	}
}

func partUploadHandler(w http.ResponseWriter, r *http.Request) {
	m, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		utils.HandleError(err)
	}
	r.ParseMultipartForm(2 << 20)
	ff, _, err := r.FormFile("data") // img is the key of the form-data
	defer ff.Close()
	name, _ := shortid.Generate()
	//	fmt.Println(handler.Filename)
	if err != nil {
		utils.HandleError(err)
	}
	var size int64 = 0
	//	fmt.Println(r.Header)
	//	r.Body = http.MaxBytesReader(w, r.Body, math.MaxInt64)
	if r.URL.Query().Get("replica") == "true" {
		//for replication - we stays original part ID/Name
		name = r.URL.Query().Get("partID")
		fmt.Printf("[INFO] Node %s sent to me replica of part %s\n", (<-cluster.Nodes.Find(r.URL.Query().Get("mainNode"))).Address, r.URL.Query().Get("partID"))
	}
	out, _ := os.OpenFile(filepath.Join(conf.DataDir, name), os.O_CREATE|os.O_WRONLY, 0666)
	defer out.Close()

	if err != nil {
		utils.HandleError(err)
	}
	size, err = io.Copy(out, ff)
	if err != nil && err != io.EOF {
		utils.HandleError(err)
	}
	p := part.Part{
		ID:             name,
		Size:           size,
		MainNodeID:     r.URL.Query().Get("mainNode"),
		ReplicaNodesID: m["replicaNode"],
		CreatedAt:      time.Now(),
	}
	n := cluster.GetCurrentNode(conf)
	n.SetUsedSpace(n.GetUsedSpace() + p.Size)
	n.SetPartsCount(n.GetPartsCount() + 1)
	partJson, _ := json.Marshal(&p)
	w.Write([]byte(utils.JsonPrettyPrint(string(partJson))))
	if r.URL.Query().Get("replica") != "true" {
		go func() {
			//for _, x := range p.ReplicaNodesID {
			p.UploadCopies(conf, nodes.All())
			//p.UploadCopies(conf, &(<-nodes.Find(p.MainNodeID)), &(<-nodes.Find(x)))
			//}
		}()
	}
	//Parts = append(Parts, p)
	//updateIndexFiles()

}
func nodeListHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	nodesJson, _ := json.Marshal(nodes.All())
	w.Write([]byte(utils.JsonPrettyPrint(string(nodesJson))))
}

func nodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	var node node.Node
	json.NewDecoder(r.Body).Decode(&node)

	if nodes.AddOrUpdateNodeInfo(conf, &node) {
		fmt.Printf("[CLUSTER] Node %s joined the cluster\n", node.Address)
	}
	(cluster.GetCurrentNode(conf)).LastUpdate = time.Now()
	//fmt.Println(r.Body)
	nodeJson, err := json.Marshal(cluster.GetCurrentNode(conf))
	if err != nil {
		utils.HandleError(err)
	}
	w.Write(nodeJson)
}

func fileUploadHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseMultipartForm(0)
	defer r.MultipartForm.RemoveAll()
	//r.Body = http.MaxBytesReader(w, r.Body, math.MaxInt64)
	r.ParseMultipartForm(2 << 20) //limiting to 2 MBytes
	ff, handler, err := r.FormFile("data")
	//fmt.Println(r.ContentLength)

	if err != nil {
		utils.HandleError(err)
		return
	}
	defer ff.Close()

	var f file.File
	fileID, _ := shortid.Generate()
	f.ID = fileID
	f.Name = handler.Filename

	var fileSizeCounter int64 = 0
	for {
		var p part.Part
		p.ID, _ = shortid.Generate()
		pr, pw := io.Pipe()
		mpw := multipart.NewWriter(pw)

		in_r := io.LimitReader(ff, int64(conf.PartChunkSize))
		var size int64 = 0
		go func() {
			var part io.Writer
			defer pw.Close()

			if part, err = mpw.CreateFormFile("data", handler.Filename); err != nil && err != io.EOF {
				utils.HandleError(err)
			}
			if size, err = io.Copy(part, in_r); err != nil && err != io.EOF {
				//	panic(err)
				utils.HandleError(err)

			}
			if err = mpw.Close(); err != nil {
				utils.HandleError(err)
			}
		}()

		var choosenNode = nodes.GetLessLoadedNode() //Nodes[rand.Intn(len(Nodes))]
		p.MainNodeID = choosenNode.ID
		p.FindNodesForReplication(conf.ReplicaCount, nodes.AllActive())
		opt := file.PartUploadOptions{
			PartID:         p.ID,
			MainNodeID:     choosenNode.ID,
			ReplicaNodesID: p.ReplicaNodesID,
		}

		v, _ := query.Values(opt)
		//fmt.Println(v.Encode())
		resp, err := http.Post(fmt.Sprintf("http://%s/part?"+v.Encode(), choosenNode.Address), mpw.FormDataContentType(), pr)
		if err != nil {
			utils.HandleError(err)
		}

		//fmt.Println(choosenNode.Address)

		err = json.NewDecoder(resp.Body).Decode(&p)
		//fmt.Println(p)

		if err != nil {
			utils.HandleError(err)
		}
		defer resp.Body.Close()
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
		if p.Size < int64(conf.PartChunkSize) {
			break
		}
	}

	//	f.Size = fileSizeCounter
	f.Size = fileSizeCounter
	//Files.List = append(Files.List, f)
	files.Add(&f)
	cluster.GetCurrentNode(conf).SetFilesCount(files.Count())
	//fmt.Println(f)
	fileJson, _ := json.Marshal(file.PublicFile{
		File:        &f,
		DownloadURL: fmt.Sprintf("http://%s/file/%s", nodes.CurrentNode(conf).Address, f.ID),
	})
	defer fmt.Printf("[INFO] Uploaded new file %s, %s\n", f.ID, f.Name)
	fj, _ := json.Marshal(f)
	//fmt.Println(string(fj))
	go nodes.SendData(conf, fj)
	go files.Save(conf.WorkingDir)

	w.Write([]byte(utils.JsonPrettyPrint(string(fileJson))))
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
		//var p = Files.FindPart(vars["id"])
		w.Header().Set("Content-Disposition", "attachment; filename="+vars["id"])
		err := DownloadPart(&w, vars["id"])
		if err != nil && err != io.EOF {
			//errorHandler(w, r, 500)
			utils.HandleError(err)
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
	var file = &file.File{}
	err := json.NewDecoder(r.Body).Decode(&file)
	if err == nil {
		files.Add(file)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	} else {
		errorHandler(w, r, 500)
	}
	cluster.GetCurrentNode(conf).SetFilesCount(files.Count())

}

func DownloadPart(w *http.ResponseWriter, id string) (err error) {
	in, err := os.Open(filepath.Join(conf.DataDir, id))
	fstat, err := in.Stat()
	var fSize = fstat.Size()
	fmt.Printf("[INFO] Sending part \"%s\", size = %d KByte(s)...\n", id, fSize/1024)
	if err != nil {
		utils.HandleError(err)
		//panic(err)
	}
	defer in.Close()
	fr := bufio.NewReader(in)

	if _, err = io.Copy(*w, fr); err != nil {
		utils.HandleError(err)
	}
	return err
}
