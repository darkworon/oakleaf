package balancing

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"oakleaf/cluster"
	"oakleaf/cluster/node"
	"oakleaf/cluster/node/client"
	"oakleaf/config"
	"oakleaf/files"
	"oakleaf/parts"
	"oakleaf/storage"
	"os"
	"sync"
	"time"

	"github.com/darkworon/oakleaf/utils"
	"github.com/google/go-querystring/query"
)

var conf = config.NodeConfig

var work_mux sync.Mutex

var isRunning = false

func Rebalance() (err error) {
	if isRunning {
		return errors.New("Error: balancing already in progress")
	}
	work_mux.Lock()
	defer work_mux.Unlock()
	isRunning = true
	defer func() { isRunning = false }()
	if (cluster.AllActive().Except(cluster.CurrentNode()).Count()) > 0 {
		//fmt.Println("Starting rebalance process...")
		if cluster.CurrentNode() != nil && (cluster.CurrentNode().GetUsedSpace()) > 0 && cluster.CurrentNode() == cluster.GetMostLoadedNode() {
			for !config.ShuttingDown {
				if cluster.CurrentNode() != cluster.GetMostLoadedNode() { //breaking up... I sent too many files :)
					break
				}
				node1 := cluster.CurrentNode()
				node2 := cluster.GetLessLoadedNode()
				var rebalanceSize = node1.GetUsedSpace() - node2.GetUsedSpace()
				p := <-LargestPossiblePart(node2, rebalanceSize)
				if p != nil {
					if p != nil {
						fmt.Printf("[BALANCE] Moving part %s, size = %d to node %s\n", p.ID, p.Size, node2.Address)
						err := MovePartTo(p, node2)
						if err != nil {
							fmt.Println(err)
						} else {
							pInfo := &parts.ChangeNode{
								PartID:    p.ID,
								OldNodeID: node1.ID,
								NewNodeID: node2.ID,
							}
							err := pInfo.ChangeNode(node1, node2)
							if err != nil {
								utils.HandleError(err)
								return err
							}
							fmt.Printf("[BALANCE] Part %s sucessfuly moved to node %s\n", p.ID, node2.Address)

						}
					} else {
						//fmt.Println("No files to make right rebalance :(")
						return
					}
				} else {
					//fmt.Println("Nothing to rebalance :(")
					return
				}
				time.Sleep(1 * time.Second)
			}
		} else {
			//fmt.Println("Error: not enough nodes in cluster to rebalance.")
		}
	}
	return err
}

func MovePartTo(p *storage.Part, n *node.Node) (err error) {
	fPath := storage.GetFullPath(p.ID)
	ratio := conf.UplinkRatio
	var size = p.Size
	fi, err := os.Open(fPath)
	if err != nil {
		storage.Delete(p)
		return err
	}
	defer fi.Close()

	pr, pw := io.Pipe()
	mpw := multipart.NewWriter(pw)

	go func() {
		var po io.Writer
		defer pw.Close()

		if po, err = mpw.CreateFormFile("data", p.ID); err != nil && err != io.EOF {
			utils.HandleError(err)
			//http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		for i := size; i > 0; i -= ratio / 10 {
			if size, err = io.CopyN(po, fi, ratio/10); err != nil && err != io.EOF {
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
				//http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}()

	opt := parts.PartUploadOptions{
		PartID:     p.ID,
		MainNodeID: cluster.CurrentNode().ID,
		Move:       true,
		//	ReplicaNodesID: p.ReplicaNodesID,
		Size: p.Size,
	}

	v, _ := query.Values(opt)
	//fmt.Println(v.Encode())
	resp, err := client.Post(fmt.Sprintf("%s://%s/parts?"+v.Encode(), n.Protocol(), n.Address), mpw.FormDataContentType(), pr)
	if err != nil {
		utils.HandleError(err)

	}
	defer resp.Body.Close()
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return
	}
	err = json.NewDecoder(resp.Body).Decode(&p)
	//choosenNode.SetCurrentJobs(choosenNode.GetCurrentJobs() - 1)
	//fmt.Println(p)

	if err != nil {
		utils.HandleError(err)
	}

	fmt.Printf("Moved parts %s to the node %s\n", p.ID, n.Address)
	storage.Delete(p)

	//	p.ReplicaNodesID = append(p.ReplicaNodesID, replicaNode.ID)
	//		p.ReplicaNodeID = replicaNode.ID
	//Parts = append(Parts, &p)
	//partJson, _ := json.Marshal(p)
	//fmt.Println(partJson)
	//	replicaNode.FilesCount++
	//choosenNode.UsedSpace += p.Size
	//replicaNode.UsedSpace += p.Size
	n.SetUsedSpace(n.GetUsedSpace() + p.Size)
	go func(_path string) {
		for { // check every 1 second if can delete files
			err := os.Remove(_path)
			if err == nil {
				return // successfully deleted files, returning
			}
			//fmt.Println("File", _path+", awaiting 3 seconds before trying to delete it again")
			time.Sleep(3 * time.Second)
		}

	}(fPath)
	return err
}

func LargestPossiblePart(node2 *node.Node, size int64) <-chan *storage.Part { // максимальный кусок, который можем отправить этой ноде
	//pl := partstorage.Parts().AscSort()
	pl := storage.All().Sort()
	pc := make(chan *storage.Part)
	p := func() {
		for _, v := range pl {
			if v.Size < size {
				if !node2.HasPart(v.ID) && !(files.All().FindPart(v.ID) != nil && files.All().FindPart(v.ID).CheckNodeExists(node2)) {
					pc <- v
					break
				}
			}
		}
		close(pc)
	}
	go p()

	return pc
}
