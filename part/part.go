package part

import (
	"errors"
	"fmt"
	"github.com/google/go-querystring/query"
	"io"
	"log"
	"mime/multipart"
	//"net/http"
	"oakleaf/cluster"
	"oakleaf/cluster/node"
	"oakleaf/cluster/node/client"
	"oakleaf/utils"
	//"oakleaf/storage"
	"oakleaf/config"
	"os"
	"path/filepath"

	//"sort"
	"time"
)

//type Config cluster.Config

type Part struct {
	PartInterface  `json:"-"`
	ID             string    `json:"id"`
	Size           int64     `json:"size"`
	CreatedAt      time.Time `json:"created_at"`
	MainNodeID     string    `json:"main_nodeID"`
	ReplicaNodeID  string    `json:"replica_nodeID,omitempty"`
	ReplicaNodesID []string  `json:"replica_nodesID,omitempty"`
}

type PartUploadOptions struct {
	PartID         string   `url:"partID"`
	Replica        bool     `url:"replica,omitempty"`
	MainNodeID     string   `url:"mainNode"`
	ReplicaNodesID []string `url:"replicaNode,omitempty"`
}

type PartInterface interface {
	IsAnyReplicaAlive() bool
	FindLiveReplica() *node.Node
	FindNodesForReplication(int, cluster.NodesList) error
	GetMainNode() *node.Node
	IsAvailable() bool
	UploadCopies(*config.Config, cluster.NodesList)
}

func (p *Part) IsAnyReplicaAlive() bool {
	for _, v := range p.ReplicaNodesID {
		if (<-cluster.Nodes.Find(v)) != nil && (<-cluster.Nodes.Find(v)).IsActive {
			return true
		}

	}
	return false
}

func (p *Part) FindLiveReplica() *node.Node {
	for _, v := range p.ReplicaNodesID {
		n := <-cluster.Nodes.Find(v)
		if n.IsActive {
			return n
		}

	}
	return nil
}

func (p *Part) GetMainNode() *node.Node {
	n := <-cluster.Nodes.Find(p.MainNodeID)
	if p != nil {
		return n
	}
	return nil
}

func (p *Part) IsAvailable() bool {
	if (<-cluster.Nodes.Find(p.MainNodeID) == nil || !(<-cluster.Nodes.Find(p.MainNodeID)).IsActive) && !p.IsAnyReplicaAlive() {
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

func (p *Part) UploadCopies(c *config.Config, nl cluster.NodesList) {
	for _, z := range p.ReplicaNodesID {
		node1 := <-nl.Find(p.MainNodeID)
		node2 := <-nl.Find(z)
		fmt.Printf("[PSINFO] Main node: %s, uploading replica to the %s...\n", node1.Address, node2.Address)
		in, err := os.Open(filepath.Join((*c).DataDir, p.ID))
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
				log.Fatal(err)
			}
			if size, err = io.Copy(part, in); err != nil && err != io.EOF {
				panic(err)

			}
			if err = mpw.Close(); err != nil {
				log.Fatal(err)
			}
		}()

		opt := PartUploadOptions{
			PartID:     p.ID,
			MainNodeID: p.MainNodeID,
			Replica:    true,
		}
		v, _ := query.Values(opt)

		resp, err := client.Post(fmt.Sprintf("%s://%s/part?"+v.Encode(), node2.Protocol(), node2.Address), mpw.FormDataContentType(), pr)
		if err != nil {
			utils.HandleError(err)
			continue
		} else {
			p.ReplicaNodesID = append(p.ReplicaNodesID, p.ReplicaNodeID)
			//go updateIndexFiles()
		}
		defer resp.Body.Close()
		//fmt.Println("Uploaded a replica!")
	}
}

/*
func (p *Part) UploadCopies(c *cluster.Config, node1 *cluster.Node, node2 *cluster.Node) {
	fmt.Printf("[PSINFO] Main node: %s, uploading replica to the %s...\n", (*node1).Address, (*node2).Address)
	in, err := os.Open(filepath.Join((*c).DataDir, p.ID))
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
			log.Fatal(err)
		}
		if size, err = io.Copy(part, in); err != nil && err != io.EOF {
			panic(err)

		}
		if err = mpw.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	opt := PartUploadOptions{
		PartID:     p.ID,
		MainNodeID: (*node1).ID,
		Replica:    true,
	}
	v, _ := query.Values(opt)

	resp, err := http.Post(fmt.Sprintf("http://%s/part?"+v.Encode(), (*node2).Address), mpw.FormDataContentType(), pr)
	if err != nil {
		utils.HandleError(err)
	} else {
		p.ReplicaNodesID = append(p.ReplicaNodesID, p.ReplicaNodeID)
		//go updateIndexFiles()
	}
	defer resp.Body.Close()
	//fmt.Println("Uploaded a replica!")
}
*/
func (p *Part) FindNodesForReplication(count int, nl cluster.NodesList) error {
	//fmt.Println("000000000")
	if len(nl.Nodes)-count <= 0 {
		return errors.New("Replica count can't be higher than count of nodes in the Cluster")
	}
	//fmt.Println("1111111")
	//fmt.Println("2222222")
	foundNodes := 0
	//fmt.Println("33333333")
	//var replicaNode *Node
	for foundNodes < count {
		var sortedList = nl.Sort()
		//fmt.Println("444444444")
		for _, n := range sortedList.Nodes {
			//	fmt.Println("555555555")
			if !p.CheckNodeExists(n) && n.IsActive {
				p.ReplicaNodesID = append(p.ReplicaNodesID, n.ID)
				foundNodes++
				break
			}
		}
		// can't find node for replica if comes here
	}
	return nil
}
