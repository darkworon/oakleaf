package balancing

import (
	"fmt"
	//"oakleaf/cluster"
	//"oakleaf/config"
	//"oakleaf/files"
	"oakleaf/cluster/node"
	"oakleaf/parts"
	"os"
	"sync"
	"time"
	//"oakleaf/cluster"
	//"net/url"
	"oakleaf/cluster"
	"oakleaf/config"
	"errors"
	"github.com/google/go-querystring/query"
	"encoding/json"
	"path/filepath"
	"mime/multipart"
	"github.com/darkworon/oakleaf/utils"
	"io"
	"github.com/darkworon/oakleaf/cluster/node/client"
)

var conf = config.NodeConfig

var work_mux sync.Mutex

var isRunning = false

func Rebalance() (err error) {
	if isRunning {
		return errors.New("Error: balancing already in progress.")
	}
	work_mux.Lock()
	defer work_mux.Unlock()
	isRunning = true
	defer func() { isRunning = false }()
	if cluster.CurrentNode() != nil && (cluster.CurrentNode().GetUsedSpace()) > 0 {
		//fmt.Println("Starting rebalance process...")
		if (cluster.AllActive().Except(cluster.CurrentNode()).Count()) > 0 {
			for {
				node1 := cluster.CurrentNode()
				node2 := cluster.GetLessLoadedNode()
				var rebalanceSize = node1.GetUsedSpace() - node2.GetUsedSpace()
				p := <-node2.LargestPossiblePart(rebalanceSize)
				if p != nil {
					// trying large files first
					fmt.Println(rebalanceSize)
					fmt.Println(conf.PartChunkSize)
					//fmt.Printf("Can rebalance to %d with node %s\n", rebalanceSize, node2.Address)

					if p != nil {
						fmt.Printf("[INFO] Moving part %s, size = %d to node %s\n", p.Name(), p.Size(), node2.Address)
						err := MovePartTo(p, node2)
						if err != nil {
							fmt.Println("Error!")
						} else {
							pInfo := &parts.ChangeNode{
								PartID:    p.Name(),
								OldNodeID: node1.ID,
								NewNodeID: node2.ID,
							}
							err := pInfo.ChangeNode(node1, node2)
							if err != nil {
								fmt.Println(err)
							} else {
								fmt.Println("Done")
							}
						}
					} else {
						//fmt.Println("No files to make right rebalance :(")
						return
					}
				} else {
					//fmt.Println("Nothing to rebalance :(")
					return
				}
				time.Sleep(500 * time.Millisecond)
			}
		} else {
			//fmt.Println("Error: not enough nodes in cluster to rebalance.")
		}
	}
	return err
}

func MovePartTo(p os.FileInfo, n *node.Node) (err error) {
	fPath := filepath.Join(conf.DataDir, p.Name())
	ratio := conf.UplinkRatio
	var size int64 = p.Size()
	fi, err := os.Open(fPath)
	if err != nil {
		return //false, err
	}
	defer fi.Close()

	pr, pw := io.Pipe()
	mpw := multipart.NewWriter(pw)

	go func() {
		var po io.Writer
		defer pw.Close()

		if po, err = mpw.CreateFormFile("data", p.Name()); err != nil && err != io.EOF {
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
		PartID:     p.Name(),
		MainNodeID: cluster.CurrentNode().ID,
		Move:       true,
		//	ReplicaNodesID: p.ReplicaNodesID,
		Size: p.Size(),
	}

	v, _ := query.Values(opt)
	//fmt.Println(v.Encode())
	resp, err := client.Post(fmt.Sprintf("%s://%s/parts?"+v.Encode(), n.Protocol(), n.Address), mpw.FormDataContentType(), pr)
	if err != nil {
		utils.HandleError(err)

	}
	if err == io.ErrUnexpectedEOF {
		return
	}

	//fmt.Println(choosenNode.Address)

	err = json.NewDecoder(resp.Body).Decode(&p)
	//choosenNode.SetCurrentJobs(choosenNode.GetCurrentJobs() - 1)
	//fmt.Println(p)

	if err != nil {
		utils.HandleError(err)
	}
	defer resp.Body.Close()
	fmt.Printf("Moved parts %s to the node %s\n", p.Name(), n.Address)
	//	p.ReplicaNodesID = append(p.ReplicaNodesID, replicaNode.ID)
	//		p.ReplicaNodeID = replicaNode.ID
	//Parts = append(Parts, &p)
	//partJson, _ := json.Marshal(p)
	//fmt.Println(partJson)
	//	replicaNode.FilesCount++
	//choosenNode.UsedSpace += p.Size
	//replicaNode.UsedSpace += p.Size
	n.SetUsedSpace(n.GetUsedSpace() + p.Size())
	go func(_path string) {
		for { // check every 1 second if can delete files
			err := os.Remove(_path)
			if err == nil {
				return // successfully deleted files, returning
			}
			fmt.Println("File", _path+", awaiting 3 seconds before trying to delete it again")
			time.Sleep(3 * time.Second)
		}

	}(fPath)
	return err
}
