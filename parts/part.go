package parts

import (
	"errors"
	"fmt"
	"github.com/google/go-querystring/query"
	"io"
	"mime/multipart"
	//"net/http"
	"oakleaf/cluster"
	"oakleaf/cluster/node"
	"oakleaf/cluster/node/client"
	//"oakleaf/parts/partstorage"
	"oakleaf/utils"
	//"oakleaf/storage"
	"bytes"
	"encoding/json"
	//"net/http"
	"oakleaf/config"
	"os"
	"path/filepath"
	"sync"
)

//type Config cluster.Config

func (p *Part) IsAnyReplicaAlive() bool {
	for _, v := range p.ReplicaNodesID {
		if (<-cluster.FindNode(v)) != nil && (<-cluster.FindNode(v)).IsActive {
			return true
		}

	}
	return false
}

func (p *Part) FindLiveReplica() *node.Node {
	for _, v := range p.ReplicaNodesID {
		n := <-cluster.FindNode(v)
		if n.IsActive {
			return n
		}

	}
	return nil
}

func (p *Part) GetMainNode() *node.Node {
	n := <-cluster.FindNode(p.MainNodeID)
	if p != nil {
		return n
	}
	return nil
}

func (p *Part) IsAvailable() bool {
	if (<-cluster.FindNode(p.MainNodeID) == nil || !(<-cluster.FindNode(p.MainNodeID)).IsActive) && !p.IsAnyReplicaAlive() {
		return false
	}
	return true
}

func (p *Part) CheckNodeExists(node *node.Node) bool {
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

func (p *Part) UploadCopies() {
	nl := cluster.AllActive()
	for _, z := range p.ReplicaNodesID {
		node1 := <-nl.Find(p.MainNodeID)
		node2 := <-nl.Find(z)
		fmt.Printf("[PSINFO] Main node: %s, uploading replica to the %s...\n", node1.Address, node2.Address)
		in, err := os.Open(filepath.Join(config.Get().DataDir, p.ID))
		if err != nil {
			utils.HandleError(err)
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
				utils.HandleError(err)
			}
			if size, err = io.Copy(part, in); err != nil && err != io.EOF {
				utils.HandleError(err)

			}
			if err = mpw.Close(); err != nil {
				utils.HandleError(err)
			}
		}()

		opt := PartUploadOptions{
			PartID:     p.ID,
			MainNodeID: p.MainNodeID,
			Replica:    true,
		}
		v, _ := query.Values(opt)

		resp, err := client.Post(fmt.Sprintf("%s://%s/parts?"+v.Encode(), node2.Protocol(), node2.Address), mpw.FormDataContentType(), pr)
		if err != nil {
			utils.HandleError(err)
			continue
		} else {
			p.ReplicaNodesID = append(p.ReplicaNodesID, p.ReplicaNodeID)
			//go updateIndexFiles()
		}
		resp.Body.Close()
		//fmt.Println("Uploaded a replica!")
	}
}

func (p *Part) ChangeNode(n1 string, n2 string) (err error) {
	if p.MainNodeID == n1 {
		p.MainNodeID = n2
		return nil
	}
	for _, x := range p.ReplicaNodesID {
		if x == n1 {
			x = n2
			return nil
		}
	}
	return errors.New(fmt.Sprintf("Couldn't change node for parts %s", p.ID))
}

func (cn *ChangeNode) ChangeNode(n1 *node.Node, n2 *node.Node) (err error) {
	var wg sync.WaitGroup
	for _, x := range cluster.AllActive().ToSlice() {
		wg.Add(1)
		go func(n *node.Node) {
			defer wg.Done()
			a, err := json.Marshal(&cn)
			if err != nil {

			}
			//fmt.Println("sending request to " + n.Address)
			req, _ := client.Post(fmt.Sprintf("%s://%s/part/info", n.Protocol(), n.Address), "application/json", bytes.NewBuffer(a))
			fmt.Println(string(a))
			if err != nil {
				return
			}
			defer req.Body.Close()
			//fmt.Println("SENT!")
		}(x)
	}
	wg.Wait()
	return err
}

func (p *Part) FindNodesForReplication(count int) error {
	nl := cluster.AllActive()
	//fmt.Println("000000000")
	if len(nl.Nodes)-count <= 0 {
		return errors.New("Replica count can't be higher than count of nodes in the Cluster")
	}
	foundNodes := 0
	for foundNodes < count {
		var sortedList = nl.Sort()
		//fmt.Println("444444444")
		for _, n := range sortedList.Nodes {
			//	fmt.Println("555555555")
			if !p.CheckNodeExists(n) && n.IsActive {
				n.SetUsedSpace(n.GetUsedSpace() + p.Size)
				p.ReplicaNodesID = append(p.ReplicaNodesID, n.ID)
				foundNodes++
				break
			}
		}
		// can't find node for replica if comes here
	}
	return nil
}
