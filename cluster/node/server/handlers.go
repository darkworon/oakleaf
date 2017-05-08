package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"oakleaf/cluster"
	"oakleaf/cluster/node"
	"oakleaf/cluster/node/client"
	"oakleaf/config"
	"oakleaf/files"
	"oakleaf/parts"
	"oakleaf/parts/partstorage"
	"oakleaf/storage"
	"oakleaf/utils"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/darkworon/oakleaf/cluster/balancing"
	"github.com/google/go-querystring/query"
	"github.com/gorilla/mux"
	"github.com/ventu-io/go-shortid"
)

var conf = config.NodeConfig
var filelist = files.All()

type works struct {
	sync.RWMutex
	InProgress int
}

///Works contains list of current api-jobs, mostly longterm, i.e. uploadings/downloadings
var Works = &works{}

func NewJob() {
	Works.Lock()
	Works.InProgress++
	Works.Unlock()
	//fmt.Println("New job.")
}

func JobOver() {
	Works.Lock()
	Works.InProgress--
	Works.Unlock()
	//fmt.Println("Job is over.")
}

func JobsCount() int {
	Works.Lock()
	a := Works.InProgress
	Works.Unlock()
	return a
}

// ToDO: FIX EVERYWHERE MAINNODE/REPLICA NODE COZ NOW NO MAIN/REPLICAS :)

var ErrClusterOutOfSpace = errors.New("error: new file could not be uploaded because cluster out of space")

func errorHandler(w http.ResponseWriter, r *http.Request, status int) {
	//w.WriteHeader(status)
	if status == http.StatusNotFound {
		http.Error(w, "404 - nothing found :(", status)
	}
	if status == http.StatusInternalServerError {
		http.Error(w, "500 Internal server error", status)
	}
}

func errorHandlerWText(w http.ResponseWriter, r *http.Request, err error) {
	//w.WriteHeader(status)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func fileListHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	w.Write([]byte(utils.JsonPrettyPrint(string(files.All().ToJson()))))
}

func fileDownloadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if config.ShuttingDown {
		if cluster.AllActive().Count() > 0 {
			n := cluster.GetLessLoadedNode()
			http.Redirect(w, r, fmt.Sprintf("%s://%s/file/%s", n.Protocol(), n.Address, vars["id"]), 302)
			log.Infof("Redirected client %s to %s", r.RemoteAddr, n.Address)
			return
		}
		http.Error(w, "521 - Server is shutting down", 521)
		return
	}
	NewJob()
	defer JobOver()

	if vars["id"] != "" {
		var f = <-files.All().Find(vars["id"])
		//w.Header().Set("Content-Length", string(f.Size))
		if f == nil {
			// not found filelist on this node - trying to get info from other nodes
			//filelist.FromJson(cluster.FindFile(vars["id"]), conf)
			f := files.File{}
			cluster.FindFile(vars["id"], f)
			filelist.Add(&f)
		}
		if f != nil {
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

func setDownloadHeaders(w http.ResponseWriter, f *files.File) {
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
	if config.ShuttingDown {
		http.Error(w, "521 - Server is shutting down", 521)
		return
	}
	vars := mux.Vars(r)
	var f = <-files.All().Find(vars["id"])
	if f != nil {
		w.WriteHeader(http.StatusOK)
		w.Write(f.ToJson())
	} else {
		errorHandler(w, r, 404)
	}
}

func partCheckExistanceHandler(w http.ResponseWriter, r *http.Request) {
	if config.ShuttingDown {
		http.Error(w, "521 - Server is shutting down", 521)
		return
	}
	vars := mux.Vars(r)
	if len(vars["id"]) == 36 {
		if _, err := os.Stat(filepath.Join(storage.GetFullPath(vars["id"]))); !os.IsNotExist(err) || partstorage.IsIn(vars["id"]) || storage.Find(vars["id"]) != nil {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("I have this part"))
			return
		}
	}
	http.Error(w, "404 - part not found", 404)
	return
}

func partUploadHandler(w http.ResponseWriter, r *http.Request) {
	if config.ShuttingDown {
		http.Error(w, "521 - Server is shutting down", 521)
		return
	}
	NewJob()
	defer JobOver()
	r.ParseMultipartForm(1 << 20)
	ff, _, err := r.FormFile("data") // img is the key of the form-data
	defer ff.Close()
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			utils.HandleError(err)
			return
		}
	}

	name := storage.NewUUID()
	var size int64 = 0
	if r.URL.Query().Get("replica") == "true" {
		//for replication - we stays original parts ID/Name
		name = r.URL.Query().Get("partID")
		log.Infof("[INFO] Node %s sent to me replica of parts %s", (<-cluster.FindNode(r.URL.Query().Get("mainNode"))).Address, r.URL.Query().Get("partID"))
	}
	if r.URL.Query().Get("move") == "true" {
		//for replication - we stays original parts ID/Name
		name = r.URL.Query().Get("partID")
		if !partstorage.IsIn(name) {
			partstorage.Add(name)
			defer partstorage.Del(name)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			fmt.Println("Already waiting for this part from another host. can't get it twice...")
			return
		}
		pSize, _ := strconv.ParseInt(r.URL.Query().Get("size"), 10, 64)
		cluster.CurrentNode().SetUsedSpace(cluster.CurrentNode().GetUsedSpace() + pSize)
		log.WithFields(log.Fields{
			"partID": r.URL.Query().Get("partID"),
		}).Infof("Node %s moving to me part", (<-cluster.FindNode(r.URL.Query().Get("mainNode"))).Address)
	}
	os.MkdirAll(storage.GetDirectory(name), os.ModePerm)
	out, _ := os.OpenFile(storage.GetFullPath(name), os.O_CREATE|os.O_WRONLY, 0666)
	defer out.Close()

	if err != nil {
		utils.HandleError(err)
	}
	size, err = io.Copy(out, ff)
	if err != nil && err != io.EOF {
		utils.HandleError(err)
	}
	p := parts.Part{
		ID:   name,
		Size: size,
		//ReplicaNodesID: m["replicaNode"],
		CreatedAt: time.Now(),
	}
	p.Nodes = append(p.Nodes, r.URL.Query().Get("mainNode"))
	if r.URL.Query().Get("replica") != "true" && r.URL.Query().Get("move") != "true" {
		p.FindNodesForReplication(config.Get().ReplicaCount)
		//p.MainNodeID = cluster.CurrentNode().ID
		go func() {
			p.UploadCopies()
		}()
	}
	_n := cluster.CurrentNode()
	storage.Add(&storage.Part{
		ID:        name,
		Path:      storage.GetFullPath(name),
		Size:      size,
		CreatedAt: time.Now(),
	})
	//n.SetUsedSpace(n.GetUsedSpace() + p.Size)
	_n.SetPartsCount(_n.GetPartsCount() + 1)
	partJson, _ := json.Marshal(&p)
	w.Write([]byte(utils.JsonPrettyPrint(string(partJson))))
	//Parts = append(Parts, p)
	//updateIndexFiles()

}
func nodeListHandler(w http.ResponseWriter, r *http.Request) {
	if config.ShuttingDown {
		http.Error(w, "521 - Server is shutting down", 521)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	nodesJson, _ := json.Marshal(cluster.Nodes())
	w.Write([]byte(utils.JsonPrettyPrint(string(nodesJson))))
}

func nodeInfoHandler(w http.ResponseWriter, r *http.Request) {
	var n = <-node.New()
	json.NewDecoder(r.Body).Decode(n)
	if cluster.AddOrUpdateNodeInfo(n) {
		log.Warnf("Node %s joined the cluster", n.Address)
		//Rebalance()
	}
	//(cluster.CurrentNode()).LastUpdate = time.Now()
	//fmt.Println(r.Body)
	nodeJson, err := json.Marshal(cluster.CurrentNode())
	if err != nil {
		utils.HandleError(err)
	}
	w.Write(nodeJson)
	r.Body.Close()
	return
}

func rebalanceHandler(w http.ResponseWriter, r *http.Request) {
	if config.ShuttingDown {
		http.Error(w, "521 - Server is shutting down", 521)
		return
	}
	switch r.Method {
	case "GET":
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		for _, x := range cluster.Nodes().Except(cluster.CurrentNode()).ToSlice() {
			resp, err := client.Post(fmt.Sprintf("%s://%s/cluster/rebalance", x.Protocol(), x.Address), "application/json; charset=utf-8", nil)
			if err != nil {
				continue
			}
			defer resp.Body.Close()
		}
		go balancing.Rebalance()
	case "POST":
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		go balancing.Rebalance()
	default:
	}

}

func fileUploadHandler(w http.ResponseWriter, r *http.Request) {
	if config.ShuttingDown {
		http.Error(w, "521 - Server is shutting down", 521)
		return
	}
	NewJob()
	defer JobOver()
	//fmt.Println(r.Header)
	var err error
	conlen, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		// maybe client not send Content-Length... Skipping
	} else {
		if cluster.SpaceAvailable() < conlen {
			err := ErrClusterOutOfSpace
			utils.HandleError(err)
			errorHandlerWText(w, r, err)
		}
	}
	var f files.File
	fileID, _ := shortid.Generate()
	f.ID = fileID
	ratio := conf.UplinkRatio
	mr, _ := r.MultipartReader()
	for {
		//fmt.Println("Asked new parts")
		fp, err := mr.NextPart()
		//fmt.Println("Got new parts")
		// This is OK, no more parts
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer fp.Close()
		// PDF 'filelist' parts
		if fp.FormName() == "file" {
			for {
				var fileSizeCounter int64 = 0
				f.Name = fp.FileName()

				var p parts.Part
				p.ID = storage.NewUUID()
				pr, pw := io.Pipe()
				mpw := multipart.NewWriter(pw)

				in_r := io.LimitReader(fp, int64(conf.PartChunkSize))
				var size int64 = 0
				go func() {
					var part io.Writer
					defer pw.Close()

					if part, err = mpw.CreateFormFile("data", f.Name); err != nil && err != io.EOF {
						utils.HandleError(err)
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}

					for i := conf.PartChunkSize; i > 0; i -= ratio / 10 {
						if size, err = io.CopyN(part, in_r, ratio/10); err != nil && err != io.EOF {
							if err == io.ErrUnexpectedEOF {
								return
							}
							utils.HandleError(err)
						}
						if err == io.EOF || size == 0 {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
					if err = mpw.Close(); err != nil {
						if err != nil {
							utils.HandleError(err)
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}
					}
				}()

				var choosenNode = cluster.GetLessLoadedNode2()
				p.Nodes = append(p.Nodes, choosenNode.ID)
				opt := parts.PartUploadOptions{
					PartID:     p.ID,
					MainNodeID: choosenNode.ID,
					//	ReplicaNodesID: p.ReplicaNodesID,
				}

				v, _ := query.Values(opt)
				//fmt.Println(v.Encode())
				resp, err := client.Post(fmt.Sprintf("%s://%s/parts?"+v.Encode(), choosenNode.Protocol(), choosenNode.Address), mpw.FormDataContentType(), pr)
				if err != nil {
					utils.HandleError(err)
				}

				if err == io.ErrClosedPipe {
					// remote is unreachable, need to mark it as unactive
				}

				//fmt.Println(choosenNode.Address)

				err = json.NewDecoder(resp.Body).Decode(&p)
				defer resp.Body.Close()
				//choosenNode.SetCurrentJobs(choosenNode.GetCurrentJobs() - 1)
				//fmt.Println(p)

				if err != nil {
					utils.HandleError(err)
				}

				log.Infof("Uploaded parts %s to the node %s", p.ID, choosenNode.Address)
				//	p.ReplicaNodesID = append(p.ReplicaNodesID, replicaNode.ID)
				//		p.ReplicaNodeID = replicaNode.ID
				f.AddPart(&p)
				fileSizeCounter += p.Size
				choosenNode.SetUsedSpace(choosenNode.GetUsedSpace() + p.Size)
				f.Size += p.Size
				if p.Size < int64(conf.PartChunkSize) {
					break
				}

			}
		}

		//Files.List = append(Files.List, f)
		if err == nil {
			filelist.Add(&f)
			cluster.CurrentNode().SetFilesCount(filelist.Count())
			//fmt.Println(f)
			fileJson, _ := json.Marshal(files.PublicFile{
				File:        &f,
				DownloadURL: fmt.Sprintf("%s://%s/file/%s", cluster.CurrentNode().Protocol(), cluster.CurrentNode().Address, f.ID),
			})
			log.WithFields(log.Fields{
				"name": f.Name,
				"id":   f.ID,
			}).Info("New file uploaded.")
			fj, _ := json.Marshal(f)
			//fmt.Println(string(fj))
			go cluster.Nodes().SendData(fj)
			go filelist.Save(conf.WorkingDir)
			w.Write([]byte(utils.JsonPrettyPrint(string(fileJson))))
			return
		}

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return

	}
}

func partDownloadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if config.ShuttingDown {
		if cluster.AllActive().Count() > 0 {
			n := cluster.GetLessLoadedNode()
			http.Redirect(w, r, fmt.Sprintf("%s://%s/part/%s", n.Protocol(), n.Address, vars["id"]), 302)
			return
		}
		http.Error(w, "521 - Server is shutting down", 521)
	}
	NewJob()
	defer JobOver()

	if vars["id"] != "" {
		//var p = Files.FindPart(vars["id"])
		w.Header().Set("Content-Disposition", "attachment; filename="+vars["id"])
		err := DownloadPart(&w, vars["id"])
		if err != nil && err != io.EOF {
			//errorHandler(w, r, 500)
			utils.HandleError(err)
			//fmt.Fprintf(w, "Not found parts with id %s", vars["id"])
			log.WithFields(log.Fields{
				"partID": vars["id"],
			}).Error("Could'n find part, returning 404.")
			//	fmt.Printf("[PSINFO] 404 - not found parts \"%s\"\n", vars["id"])
		}
	}

}

func partDeleteHandler(w http.ResponseWriter, r *http.Request) {
}

func fileDeleteHandler(w http.ResponseWriter, r *http.Request) {
}

func getFileInfoHandler(w http.ResponseWriter, r *http.Request) {
	if config.ShuttingDown {
		http.Error(w, "521 - Server is shutting down", 521)
		return
	}
	var file = &files.File{}
	err := json.NewDecoder(r.Body).Decode(&file)
	if err == nil {
		filelist.Add(file)
		go filelist.Save(conf.WorkingDir)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	} else {
		errorHandler(w, r, 500)
	}
	cluster.CurrentNode().SetFilesCount(filelist.Count())

}

func changePartInfoHandler(w http.ResponseWriter, r *http.Request) {
	NewJob()
	defer JobOver()
	//fmt.Println("Changing info")
	var cn = &parts.ChangeNode{}
	err := json.NewDecoder(r.Body).Decode(&cn)
	if err == nil {
		p := filelist.FindPart(cn.PartID)
		if p != nil {
			p.ChangeNode(cn.OldNodeID, cn.NewNodeID)
			//fmt.Printf("Changed node for part %s: from %s to %s\n", p.ID, cn.OldNodeID, cn.NewNodeID)
			go filelist.Save(conf.WorkingDir)
		}
		//fmt.Println(p)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))

	} else {
		utils.HandleError(err)
		// couldn't change filelist info
		errorHandler(w, r, 500)
	}

}

func DownloadPart(w *http.ResponseWriter, id string) (err error) {
	if config.ShuttingDown {
		http.Error(*w, "521 - Server is shutting down", 521)
		return
	}
	NewJob()
	defer JobOver()
	in, err := os.OpenFile(storage.GetFullPath(id), os.O_RDONLY|os.O_EXCL, 0)
	fstat, err := in.Stat()
	var fSize = fstat.Size()
	fmt.Printf("[INFO] Sending parts \"%s\", size = %d KByte(s)...\n", id, fSize/1024)
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
