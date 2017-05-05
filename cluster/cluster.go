package cluster

import (
	//	"oakleaf/file"
	//"fmt"
	"encoding/json"
	"fmt"
	"io"
	//"net/http"
	"oakleaf/cluster/node"
	"oakleaf/cluster/node/client"
	"oakleaf/config"
	//"oakleaf/common/types"
	//"crypto/tls"
	//"oakleaf/cluster/node/client"
	//"oakleaf/utils"
	"sort"
	//"strings"
	"sync"
	"time"
)

type NodesList struct {
	NodesListInterface
	Nodes []*node.Node
	sync.RWMutex
}

type NodesListInterface interface {
	Find() <-chan *node.Node
	GetLessLoadedNode() *node.Node
}

var Nodes = &NodesList{}

var conf = config.NodeConfig

/*func (n NodesList) FindNode(value string) *Node {

	for _, v := range n.list {
		if v.ID == value || v.Address == value {
			return v
		}
	}
	return nil
}
*/

func (nl *NodesList) Add(n *node.Node) {
	_n := <-nl.Find(n.ID)
	nl.Lock()
	if _n == nil {
		nl.Nodes = append(nl.Nodes, n)
		//go fl.Save()
		//nl.Save()
		fmt.Println(nl.Nodes)
	}
	defer nl.Unlock()
}

func (nl *NodesList) Count() (n int) {
	nl.Lock()
	defer nl.Unlock()
	return len(nl.Nodes)
}

func (ns *NodesList) NodeExists(n *node.Node) bool {
	for _, x := range ns.Nodes {
		if x.ID == n.ID {
			return true
		}
	}
	return false
}

func (nl *NodesList) AddOrUpdateNodeInfo(conf *config.Config, node *node.Node) (joined bool) {
	if !node.IsEmpty() {
		if !nl.NodeExists(node) {
			joined = true
			nl.Add(node)
			if !conf.NodeExists(node.Address) {
				conf.ClusterNodes = append(conf.ClusterNodes, node.Address)
			}
		} else if node.ID != (nl.CurrentNode(conf)).ID {
			joined = false
			n := <-nl.Find(node.ID)
			n.Update(node)
		}

		//Nodeconfig.Config.Save()
		conf.Save()
		//nodesInfoWorker()
		return joined
	}
	return false
}

func (n *NodesList) Find(value string) <-chan *node.Node {
	nc := make(chan *node.Node)
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

func (n *NodesList) CurrentNode(c *config.Config) *node.Node {
	return <-n.Find(c.NodeID)
}

func GetCurrentNode(c *config.Config) *node.Node {
	n := <-Nodes.Find(c.NodeID)
	return n
}

func (nl *NodesList) GetLessLoadedNode() *node.Node {
	var nodesListSorted []*node.Node
	//nl.Lock()
	nodesListSorted = append(nodesListSorted, nl.Nodes...)
	//nl.Unlock()

	sort.Slice(nodesListSorted, func(i, j int) bool {
		return (nodesListSorted[i]).UsedSpace < (nodesListSorted[j]).UsedSpace
	})
	fmt.Println(nodesListSorted[0].Address)
	return nodesListSorted[0]

}

func (nl *NodesList) GetLessLoadedNode2() *node.Node {
	var nodesListSorted []*node.Node
	nl.Lock()
	nodesListSorted = append(nodesListSorted, nl.Nodes...)

	sort.Slice(nodesListSorted, func(i, j int) bool {
		return (nodesListSorted[i]).CurrentJobs() < (nodesListSorted[j]).CurrentJobs()
	})
	fmt.Println(nodesListSorted[0].Address)
	nodesListSorted[0].SetCurrentJobs(nodesListSorted[0].CurrentJobs() + 1)
	nl.Unlock()
	return nodesListSorted[0]

}

func (nl *NodesList) AllExcept(n *node.Node) []*node.Node {
	var tempList = []*node.Node{}
	for _, v := range nl.Nodes {
		if v.ID != n.ID {
			tempList = append(tempList, v)
		}
	}
	return tempList
}

func (nl *NodesList) GetCurrentNode(c *config.Config) *node.Node {
	return <-nl.Find(c.NodeID)
}

func (nl *NodesList) RefreshNodesList(c *config.Config) {
	var wg sync.WaitGroup
	for _, n := range c.ClusterNodes {
		wg.Add(1)
		go func(x string) {
			defer wg.Done()
			_node, err := nodeInfoExchange(c, x)
			if err != nil && _node == nil {
				//utils.HandleError(err)
			} else {
				nl.AddOrUpdateNodeInfo(c, _node)

			}
		}(n)
	}
	wg.Wait()
}

/*
func nodeInfoExchange(c *config.Config, address string) (node *node.Node, err error) {
	var proto = "https"
	a, err := client.Head(fmt.Sprintf("https://%s/", address))
	if err != nil && strings.Contains(err.Error(), "HTTP") {
		proto = "http"
	} else {
		defer a.Body.Close()
	}
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		err := json.NewEncoder(w).Encode(GetCurrentNode(c))
		if err != nil {
		}
	}()
	resp, err := client.Post(fmt.Sprintf("%s://%s/node/info", proto, address), "application/json; charset=utf-8", r)
	defer resp.Body.Close()
	if err != nil {
		//fmt.Println(err.Error())
		//fmt.Println("111111234455")
		return nil, err
	}

	json.NewDecoder(resp.Body).Decode(&node)
	return node, err
}*/

func nodeInfoExchange(c *config.Config, address string) (n *node.Node, err error) {
	var proto = "http"
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		err := json.NewEncoder(w).Encode(GetCurrentNode(c))
		if err != nil {
			//utils.HandleError(err)
			return
		}
	}()
	resp, err := client.Post(fmt.Sprintf("%s://%s/node/info", proto, address), "application/json; charset=utf-8", r)
	if err != nil {
		//utils.HandleError(err)
		return nil, err
		//log.Fatal(err)
	}
	n = &node.Node{}
	resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&n)
	return n, err
}

func NodeInfoExchange(c *config.Config, address string) (*node.Node, error) {
	return nodeInfoExchange(c, address)
}

func (nl NodesList) SendData(c *config.Config, data []byte) {
	var wg sync.WaitGroup
	for _, v := range nl.AllExcept(nl.CurrentNode(c)) {
		wg.Add(1)
		go func(n *node.Node) {
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

func (nl NodesList) FindFile(c *config.Config, fId string, out interface{}) {
	//dc := make(chan []byte)
	//nl.Lock()
	//defer nl.Unlock()
	//go func(n NodesList) {
	for _, v := range nl.AllExcept(nl.GetCurrentNode(c)) {
		err := v.GetFileJson(fId, &out)
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

	sort.Slice(nl2.Nodes, func(i, j int) bool {
		return (*nl2.Nodes[i]).CurrentJobs() < (*nl2.Nodes[j]).CurrentJobs()
	})
	return nl2
}

func (nl *NodesList) All() (nl2 NodesList) {
	nl.Lock()
	nl2.Nodes = append(nl2.Nodes, nl.Nodes...)
	nl.Unlock()
	return nl2
}

func (nl *NodesList) AllActive() (nl2 NodesList) {
	nl.Lock()
	for _, x := range nl.Nodes {
		if x.IsActive {
			nl2.Nodes = append(nl2.Nodes, x)
		}
	}
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
func NewNode(id string, name string, address string, totalSpace int64, usedSpace int64, tls bool) *node.Node {
	var n node.Node
	//node := node.Node{id, name, address, true, 31457280, 0, 0, 0, time.Now()}
	n.ID = id
	n.Name = name
	n.Address = address
	n.IsActive = true
	n.TotalSpace = totalSpace
	n.UsedSpace = usedSpace
	n.LastUpdate = time.Now()
	n.TLS = tls

	return &n
}

func New() <-chan *node.Node {
	nc := make(chan *node.Node)
	nc <- &node.Node{}
	close(nc)
	return nc
}
