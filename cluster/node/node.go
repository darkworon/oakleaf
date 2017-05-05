package node

import (
	//	"oakleaf/cluster"
	"oakleaf/config"
	//"oakleaf/file"
	"oakleaf/cluster/node/client"
	//"oakleaf/node/server"
	"errors"
	//"net/http"
	"sync"
	"time"
	//"oakleaf/node"
	//"oakleaf/utils"
	"bytes"
	"encoding/json"
	"fmt"
)

type Config *config.Config

type Node struct {
	nodeInterface `json:"-"`
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Address       string    `json:"address"`
	IsActive      bool      `json:"is_active"`
	TotalSpace    int64     `json:"total_space"`
	UsedSpace     int64     `json:"used_space"`
	FilesCount    int       `json:"files_count"`
	PartsCount    int       `json:"parts_count"`
	currentJobs   int       `json:"current_jobs"`
	LastUpdate    time.Time `json:"last_update"`
	TLS           bool      `json:"tls"`
	sync.RWMutex  `json:"-"`
	//Parts      []string  `json:"parts,omitempty"`
}

type nodeInterface interface {
	Update(*Node)
	SendData([]byte) error
	GetFilesCount() int
	SetFilesCount(int)
	GetPartsCount()
	SetPartsCount(int)
	GetUsedSpace() int64
	SetUsedSpace(int64)
	Protocol() string
}

func (n *Node) Update(n2 *Node) {
	n.Lock()
	go func(old, new *Node) {
		defer n.Unlock()
		if !n.IsActive && n2.IsActive {
			defer fmt.Printf("[CLUSTER] Node %s -> active\n", n.Address)
		}
		n.Name = n2.Name
		n.IsActive = n2.IsActive
		n.TotalSpace = n2.TotalSpace
		n.UsedSpace = n2.UsedSpace
		n.FilesCount = n2.FilesCount
		n.PartsCount = n2.PartsCount
		n.TLS = n2.TLS
	}(n, n2)

}

func (c *Node) Protocol() string {
	if c.TLS {
		return "https"
	} else {
		return "http"
	}
}

func (n *Node) GetFilesCount() int {
	n.Lock()
	defer n.Unlock()
	return n.FilesCount

}

func (n *Node) SetFilesCount(a int) {
	n.Lock()
	n.FilesCount = a
	n.Unlock()

}

func (n *Node) GetPartsCount() int {
	n.Lock()
	defer n.Unlock()
	return n.PartsCount
}

func (n *Node) SetPartsCount(a int) {
	n.Lock()
	n.PartsCount = a
	n.Unlock()
}

func (n *Node) SetUsedSpace(a int64) {
	n.Lock()
	n.UsedSpace = a
	n.Unlock()
}

func (n *Node) GetUsedSpace() int64 {
	n.Lock()
	defer n.Unlock()
	return n.UsedSpace
}

func (n *Node) CurrentJobs() int {
	n.Lock()
	defer n.Unlock()
	return n.currentJobs
}

func (n *Node) SetCurrentJobs(a int) {
	n.Lock()
	n.currentJobs = a
	defer n.Unlock()
}

func (n *Node) SendData(data []byte) (err error) {
	if n.IsActive {
		err = n.SendFileInfo(data)
	} else {
		err = errors.New(fmt.Sprintf("Error: node %s is not active", n.Address))
	}
	return err
}

func (n *Node) GetFileJson(fileId string, out *interface{}) error {
	resp, _ := client.Get(fmt.Sprintf("%s://%s/file/info/%s", n.Protocol(), n.Address, fileId))
	defer resp.Body.Close()
	err := json.NewDecoder(resp.Body).Decode(&out)
	return err
}

func (n *Node) SendFileInfo(data []byte) error {
	resp, err := client.Post(fmt.Sprintf("%s://%s/file/info", n.Protocol(), n.Address), "application/json", bytes.NewBuffer(data))
	defer resp.Body.Close()
	return err

}

func (n *Node) IsEmpty() bool {
	n.Lock()
	defer n.Unlock()
	if n.ID == "" || n.Address == "" {
		return true
	}
	return false
}

func New() <-chan *Node {
	nc := make(chan *Node)
	go func() {
		defer close(nc)
		nc <- &Node{}
	}()
	return nc
}

/*func (n *Node) IsActive() bool {
	n.Lock()
	defer n.Unlock()
	return n.IsActive
}*/

/*

 */
