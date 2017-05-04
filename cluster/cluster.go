package cluster

import (
	//	"oakleaf/file"
	//"fmt"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"oakleaf/config"
	"oakleaf/node"
	//"oakleaf/common/types"
	"oakleaf/node/client"
	"oakleaf/utils"
	"sort"
	"sync"
	"time"
)

type Node *node.Node

type Config config.Config

type NodesList struct {
	NodesListInterface
	Nodes []Node
	*sync.RWMutex
}

type NodesListInterface interface {
	Find() <-chan Node
	GetLessLoadedNode() Node
}

var currentNode Node
var Nodes = NodesList{}

/*func (n NodesList) FindNode(value string) *Node {

	for _, v := range n.list {
		if v.ID == value || v.Address == value {
			return v
		}
	}
	return nil
}
*/

func (nl *NodesList) Add(n Node) {
	_n := <-nl.Find(n.ID)
	nl.Lock()
	defer nl.Unlock()
	if _n == nil {
		nl.Nodes = append(nl.Nodes, n)
		//go fl.Save()
	}
}

func (nl *NodesList) Count() (n int) {
	nl.Lock()
	n = len(nl.Nodes)
	nl.Unlock()
	return n
}

func (c *Config) NodeExists(n Node) bool {
	for _, x := range c.ClusterNodes {
		if x == n.Address {
			return true
		}
	}
	return false
}

func (ns *NodesList) NodeExists(n Node) bool {
	for _, x := range ns.Nodes {
		if x.ID == n.ID {
			return true
		}
	}
	return false
}

func (nl *NodesList) AddOrUpdateNodeInfo(conf *Config, node Node) (joined bool) {
	if !nl.NodeExists(node) {
		joined = true
		nl.Add(node)
		if !conf.NodeExists(node) {
			conf.ClusterNodes = append(conf.ClusterNodes, node.Address)
		}
	} else if node.ID != (nl.CurrentNode(conf)).ID {
		joined = false
		n := <-nl.Find(node.ID)
		n.Update(node)
	}
	//NodeConfig.Save()
	//nodesInfoWorker()
	return joined
}

func (n *NodesList) Find(value string) <-chan Node {
	nc := make(chan Node)
	f := func() {
		n.Lock()
		defer n.Unlock()
		for _, v := range n.Nodes {
			if v.ID == value || v.Address == value {
				nc <- v
			}
		}
		close(nc)
	}
	go f()
	return nc
}

func (n *NodesList) CurrentNode(c *Config) Node {
	return <-n.Find(c.NodeID)
}

func GetCurrentNode(c *Config) Node {
	return <-Nodes.Find(c.NodeID)
}

func (nl *NodesList) GetLessLoadedNode() Node {
	var nodesListSorted []Node
	nl.Lock()
	nodesListSorted = append(nodesListSorted, nl.Nodes...)
	nl.Unlock()

	sort.Slice(&nodesListSorted, func(i, j int) bool {
		return nodesListSorted[i].UsedSpace < nodesListSorted[j].UsedSpace
	})
	return nodesListSorted[0]

}

func (nl *NodesList) AllExcept(n Node) []Node {
	var tempList = []Node{}
	for _, v := range nl.Nodes {
		if v.ID != n.ID {
			tempList = append(tempList, v)
		}
	}
	return tempList
}

func (nl *NodesList) GetCurrentNode(c *Config) Node {
	currentNode = <-nl.Find(c.NodeID)
	return currentNode
}

func (nl *NodesList) RefreshNodesList(c *Config) {
	var wg sync.WaitGroup
	for _, n := range c.ClusterNodes {
		wg.Add(1)
		go func(x string) {
			defer wg.Done()
			_node, err := nodeInfoExchange(c, x)
			if err != nil || _node == nil {
				utils.HandleError(err)
			} else {
				nl.AddOrUpdateNodeInfo(c, _node)

			}
		}(n)
	}
	wg.Wait()
}

func nodeInfoExchange(c *Config, address string) (node Node, err error) {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		err := json.NewEncoder(w).Encode(GetCurrentNode(c))
		if err != nil {
		}
	}()
	resp, err := http.Post(fmt.Sprintf("http://%s/node/info", address), "application/json; charset=utf-8", r)
	if err != nil {
		//HandleError(err)
		return nil, err
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&node)
	return node, err
}

func NodeInfoExchange(c *Config, address string) (node Node, err error) {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		err := json.NewEncoder(w).Encode(GetCurrentNode(c))
		if err != nil {
		}
	}()
	resp, err := http.Post(fmt.Sprintf("http://%s/node/info", address), "application/json; charset=utf-8", r)
	if err != nil {
		//HandleError(err)
		return nil, err
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&node)
	return node, err
}

func (nl NodesList) SendData(c *Config, data []byte) {
	var wg sync.WaitGroup
	for _, v := range nl.AllExcept(nl.CurrentNode(c)) {
		wg.Add(1)
		go func(n Node) {
			defer wg.Done()
			err := n.SendData(data)
			if err != nil {
				// todo: handler
			}
		}(v)
		wg.Wait()
	}
}

func (nl NodesList) ToJson() <-chan []byte {
	nc := make(chan []byte)
	nl.Lock()
	defer nl.Unlock()
	go func(cl NodesList) {
		a, err := json.Marshal(cl.Nodes)
		if err != nil {
			// todo: handler
		}
		nc <- a
		close(nc)
	}(nl)
	return nc
}

func (nl NodesList) FindFile(c *Config, fId string, out interface{}) {
	//dc := make(chan []byte)
	//nl.Lock()
	//defer nl.Unlock()
	//go func(n NodesList) {
	for _, v := range nl.AllExcept(nl.GetCurrentNode(c)) {
		err := client.GetFileJson(v.Address, fId, &out)
		if err != nil {
			fmt.Println(out)
		}
	}
	//	close(dc)
	//}(nl)

}

func (nl *NodesList) Sort() (nl2 NodesList) {
	nl.Lock()
	nl2.Nodes = append(nl2.Nodes, nl.Nodes...)
	nl.Unlock()

	sort.Slice(&nl2.Nodes, func(i, j int) bool {
		return nl2.Nodes[i].UsedSpace < nl2.Nodes[j].UsedSpace
	})
	return
}

func (nl *NodesList) All() (nl2 NodesList) {
	nl.Lock()
	nl2.Nodes = append(nl2.Nodes, nl.Nodes...)
	nl.Unlock()
	return nl2
}

/*
func newFileNotify(jsonData []byte, err error) {

	var wg sync.WaitGroup
	//	fmt.Println(Nodes.GetCurrentNode())
	//fmt.Println(Nodes[1:])
	for _, v := range AllExcept(GetCurrentNode()) {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			if n.IsActive {
				resp, err := http.Post(fmt.Sprintf("http://%s/file/info", n.Address), "application/json", bytes.NewBuffer(jsonData))
				if err != nil {
					HandleError(err)
				}
				defer resp.Body.Close()
			}
		}(v)
		wg.Wait()
	}
}*/
func NewNode(id string, name string, address string, totalSpace int64, usedSpace int64) Node {
	var n node.Node
	//node := node.Node{id, name, address, true, 31457280, 0, 0, 0, time.Now()}
	n.ID = id
	n.Name = name
	n.Address = address
	n.IsActive = true
	n.TotalSpace = totalSpace
	n.UsedSpace = usedSpace
	n.LastUpdate = time.Now()

	return &n
}
