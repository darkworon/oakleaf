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
	"oakleaf/utils"
	//"oakleaf/storage"
	"bytes"
	"encoding/json"
	//"net/http"
	"os"
	"sync"
	"github.com/darkworon/oakleaf/storage"
	"io/ioutil"
	"net/http"
)

//type Config cluster.Config

func (p *Part) IsAnyNodeAlive() bool {
	for _, v := range p.Nodes {
		if (<-cluster.FindNode(v)) != nil && (<-cluster.FindNode(v)).IsActive {
			return true
		}

	}
	return false
}

func (p *Part) FindLiveNode() *node.Node {
	for _, v := range p.Nodes {
		n := <-cluster.FindNode(v)
		if n.IsActive {
			return n
		}

	}
	return nil
}

func (p *Part) GetMainNode() *node.Node {
	n := <-cluster.FindNode(p.Nodes[0])
	if p != nil {
		return n
	}
	return nil
}

func (p *Part) IsAvailable() bool {
	if (<-cluster.FindNode(p.Nodes[0]) == nil || !(<-cluster.FindNode(p.Nodes[0])).IsActive) && !p.IsAnyNodeAlive() {
		return false
	}
	return true
}

func (p *Part) CheckNodeExists(node *node.Node) bool {
	for _, n := range p.Nodes {
		if n == node.ID {
			return true
		}
	}
	return false
}

func (p *Part) UploadCopies() {
	nl := cluster.AllActive()
	for _, z := range p.Nodes[1:] {
		node1 := <-nl.Find(p.Nodes[0])
		node2 := <-nl.Find(z)
		fmt.Printf("[PSINFO] Main node: %s, uploading replica to the %s...\n", node1.Address, node2.Address)
		in, err := os.Open(storage.GetFullPath(p.ID))
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
			MainNodeID: p.Nodes[0],
			Replica:    true,
		}
		v, _ := query.Values(opt)

		resp, err := http.Post(fmt.Sprintf("%s://%s/parts?"+v.Encode(), node2.Protocol(), node2.Address), mpw.FormDataContentType(), pr)
		if err != nil {
			utils.HandleError(err)
		}
		if resp != nil {
			_, _ = ioutil.ReadAll(resp.Body)
			defer resp.Body.Close()
			p.Nodes = append(p.Nodes, p.Nodes[1])
			//go updateIndexFiles()
		}

		//fmt.Println("Uploaded a replica!")
	}
}

func (p *Part) ChangeNode(n1 string, n2 string) (err error) {
	//fmt.Printf("Started changing node from %s to %s for part %s\n",n1, n2, p.ID)
	for i, x := range p.Nodes {
		if x == n1 {
			p.Nodes[i] = n2
			return nil
		}
	}
	fmt.Println("NAH(")
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
			//for x:=0; x < 3; x++ { // making 3 attemps
				//fmt.Println("sending request to " + n.Address)
				req, err := http.Post(fmt.Sprintf("%s://%s/part/info", n.Protocol(), n.Address), "application/json", bytes.NewBuffer(a))
				//fmt.Println(string(a) + " -> " + string(n.Address))
				if err != nil {
					utils.HandleError(err)
				}
				if req != nil {
					defer req.Body.Close()
					if err == nil && req.StatusCode == 200 {
						return
					}
				}
				//time.Sleep(2*time.Second)
			//}
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
				p.Nodes = append(p.Nodes, n.ID)
				foundNodes++
				break
			}
		}
		// can't find node for replica if comes here
	}
	return nil
}