package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
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

const (
	JSONType = "application/json"
	HTMLType = "text/html"
)

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

var basicPath = os.Getenv("GOPATH") + "/src/github.com/darkworon/oakleaf/web/templates/"

//var templates = template.Must(template.ParseFiles(os.Getenv("GOPATH") + "/src/github.com/darkworon/oakleaf/web/tmpl/upload.html"))

func render(w http.ResponseWriter, tmpl string, data interface{}) {
	var templates = template.Must(template.ParseGlob(basicPath + "*"))
	err := templates.ExecuteTemplate(w, tmpl+".html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func renderJson(w http.ResponseWriter, tmpl string, data interface{}) {
	t, err := template.ParseFiles(basicPath + tmpl + ".json")
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	t.ExecuteTemplate(w, basicPath+tmpl+".json", data)
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

func errorHandlerWText(w http.ResponseWriter, r *http.Request, err error) {
	//w.WriteHeader(status)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func indexPageHandler(w http.ResponseWriter, r *http.Request) {
	render(w, "index", nil)
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
	if ff != nil {
		defer ff.Close()
	}
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// client disconnected, need to remove file
			// TODO: FILE REMOVAL
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
	out, err := os.OpenFile(storage.GetFullPath(name), os.O_CREATE|os.O_WRONLY, 0666)
	if out != nil {
		defer out.Close()
	}
	if err != nil {
		utils.HandleError(err)
	}
	if ff != nil && out != nil {
		size, err = io.Copy(out, ff)
		if err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF { // TODO IF SOME BUG - FIX IT
				os.Remove(storage.GetFullPath(name))
				return
			}
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
	} else {
		log.Errorf("Remote peer %s disconnected, uploading of part %s cancelled", r.RemoteAddr, name)
		os.Remove(storage.GetFullPath(name))
	}

}
func nodeListHandlerAPI(w http.ResponseWriter, r *http.Request) {
	if config.ShuttingDown {
		http.Error(w, "521 - Server is shutting down", 521)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(<-cluster.Nodes().SortBy("Address").ToJson())

}

func nodeListHandler(w http.ResponseWriter, r *http.Request) {
	if config.ShuttingDown {
		http.Error(w, "521 - Server is shutting down", 521)
		return
	}
	render(w, "cluster", cluster.Nodes().SortBy("Address").ToSlice())
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

func nodeExitClusterHanderAPI(w http.ResponseWriter, r *http.Request) {
	go ExitCluster()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
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
		for _, x := range cluster.Nodes().ToSlice() {
			resp, err := client.Post(fmt.Sprintf("%s://%s/api/cluster/rebalance", x.Protocol(), x.Address), "application/json; charset=utf-8", nil, 3*time.Second)
			if err != nil {
				continue
			}
			if resp != nil {
				defer resp.Body.Close()
			}
		}
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
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if fp != nil {
			//defer fp.Close()
		}
		//fmt.Println("Got new parts")
		// This is OK, no more parts
		if err == io.EOF {
			break
		}
		if fp.FileName() == "" {
			continue
		}
		// PDF 'filelist' parts
		if fp.FormName() == "files[]" {
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
				resp, err := client.Post(fmt.Sprintf("%s://%s/api/parts?"+v.Encode(), choosenNode.Protocol(), choosenNode.Address), mpw.FormDataContentType(), pr, 10*time.Minute)
				if resp != nil {
					defer resp.Body.Close()
				}
				if err != nil {
					utils.HandleError(err)
				}

				if err == io.ErrClosedPipe {
					// remote is unreachable, need to mark it as unactive
				}

				//fmt.Println(choosenNode.Address)

				err = json.NewDecoder(resp.Body).Decode(&p)

				//choosenNode.SetCurrentJobs(choosenNode.GetCurrentJobs() - 1)
				//fmt.Println(p)

				if err != nil {
					utils.HandleError(err)
				}

				log.Infof("Uploaded part %s to the node %s", p.ID, choosenNode.Address)
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
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}

		//Files.List = append(Files.List, f)
		if err == nil {
			filelist.Add(&f)
			cluster.CurrentNode().SetFilesCount(filelist.Count())
			//fmt.Println(f)
			pf1 := files.PublicFile{
				File:         &f,
				DownloadURL:  fmt.Sprintf("%s://%s/file/%s", cluster.CurrentNode().Protocol(), cluster.CurrentNode().Address, f.ID),
				DeleteURL:    fmt.Sprintf("%s://%s/file/%s", cluster.CurrentNode().Protocol(), cluster.CurrentNode().Address, f.ID),
				DeleteMethod: "DELETE",
			}
			pf := files.PublicFiles{}
			pf.Files = append(pf.Files, pf1)
			fileJSON, _ := json.Marshal(pf)
			log.Debug(string(fileJSON))
			log.WithFields(log.Fields{
				"name": f.Name,
				"id":   f.ID,
			}).Info("New file uploaded")
			fj, _ := json.Marshal(f)
			//fmt.Println(string(fj))
			go cluster.Nodes().SendData(fj)
			go filelist.Save(conf.WorkingDir)
			w.Write([]byte(utils.JsonPrettyPrint(string(fileJSON))))
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
			log.Debugf("Changed node for part %s: from %s to %s", p.ID, cn.OldNodeID, cn.NewNodeID)
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

func ExitCluster() {
	log.Infoln("Exiting...")
	cluster.CurrentNode().IsActive = false
	cluster.CurrentNode().SetStatus(node.StateShuttingDown)
	if cluster.AllActive().Count() > config.Get().ReplicaCount {
		//delete index file if i'm not alone... ask it after join from another node
		//log.Infoln("Removing index file...")
		//os.Remove(filepath.Join(config.Get().WorkingDir, indexFileName))
		log.Info("Moving all stored data to other nodes.")
		balancing.MoveAllData()
		time.Sleep(3 * time.Second)
	}
	config.Save()
	Stop <- true
	close(Stop)
	log.Infoln("Awaiting all processes done...")
	<-Stopped
	os.Exit(1)
}
