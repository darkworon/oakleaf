package node

import (
	//	"oakleaf/cluster"
	"oakleaf/config"
	//"oakleaf/file"
	"oakleaf/node/client"
	//"oakleaf/node/server"
	"fmt"
	"sync"
	"time"
)

type Config *config.Config

type Node struct {
	NodeInterface `json:-`
	ID            string        `json:"id"`
	Name          string        `json:"name"`
	Address       string        `json:"address"`
	IsActive      bool          `json:"is_active"`
	TotalSpace    int64         `json:"total_space"`
	UsedSpace     int64         `json:"used_space"`
	FilesCount    int           `json:"files_count"`
	PartsCount    int           `json:"parts_count"`
	LastUpdate    time.Time     `json:"last_update"`
	locker        *sync.RWMutex `json:"-"`
	//Parts      []string  `json:"parts,omitempty"`
}

type NodeInterface interface {
	Update(*Node)
	SendData([]byte) error
}

func (n *Node) Update(n2 *Node) {
	n.locker.Lock()
	if !n.IsActive && n2.IsActive {
		defer fmt.Printf("[CLUSTER] Node %s -> active\n", n.Address)
	}
	n.IsActive = n2.IsActive
	n.TotalSpace = n2.TotalSpace
	n.UsedSpace = n2.UsedSpace
	n.FilesCount = n2.FilesCount
	*n = *n2
	n = n2
	n.locker.Unlock()

}

func (n *Node) SendData(data []byte) (err error) {
	if n.IsActive {
		err = client.SendFileInfo(n.Address, data)
	}
	return err
}

func New(n chan Node) {
	n <- Node{}
}

/*func (n *Node) IsActive() bool {
	n.locker.Lock()
	defer n.locker.Unlock()
	return n.IsActive
}*/

/*

 */
