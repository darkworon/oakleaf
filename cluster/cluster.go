package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"oakleaf/cluster/node"
	"oakleaf/cluster/node/client"
	"oakleaf/config"
	"sort"
	"sync"
	"github.com/ventu-io/go-shortid"
	"strconv"
	"github.com/darkworon/oakleaf/utils"
)

type ClusterNodes struct {
	sync.RWMutex  `json:"-"`
	Nodes []*node.Node `json:"nodes"`
}

func Nodes() *ClusterNodes {
	return nodes.All()
}

func SpaceAvailable() (space int64) {
	for _, x := range AllActive().ToSlice() {
		space += x.TotalSpace - x.GetUsedSpace()
	}
	return space
}

func (nl *ClusterNodes) All() *ClusterNodes {
	nl2 := <-New()
	nl.Lock()
	defer nl.Unlock()
	nl2.Nodes = append(nl2.Nodes, nl.Nodes...)
	return nl2
}

func (nl *ClusterNodes) AllActive() *ClusterNodes {
	nl2 := <-New()
	nl.Lock()
	for _, x := range nl.Nodes {
		if x.IsActive {
			nl2.Nodes = append(nl2.Nodes, x)
		}
	}
	nl.Unlock()
	return nl2
}

func AllActive() *ClusterNodes {
	return nodes.AllActive()
}

func New() <-chan *ClusterNodes {
	nc := make(chan *ClusterNodes)
	go func() {
		defer close(nc)
		nc <- &ClusterNodes{}
	}()
	return nc
}

func (nl *ClusterNodes) Add(n *node.Node) {
	_n := <-nl.Find(n.ID)
	nl.Lock()
	if _n == nil {
		nl.Nodes = append(nl.Nodes, n)
		//fmt.Println(nl.Nodes)
	}
	defer nl.Unlock()
}

func (me *ClusterNodes) Count() int {
	me.Lock()
	defer me.Unlock()
	return len(me.Nodes)
}

func (ns *ClusterNodes) IsNodeExists(n *node.Node) bool {
	for _, x := range ns.Nodes {
		if x.ID == n.ID {
			return true
		}
	}
	return false
}

func FlushNodesCounters() {
	for _, x := range AllActive().ToSlice() {
		x.SetCurrentJobs(0)
	}
}

func (nl *ClusterNodes) AddOrUpdateNodeInfo(conf *config.Config, node *node.Node) (joined bool) {
	if !node.IsEmpty() {
		if !nl.IsNodeExists(node) {
			nl.Add(node)
			if !conf.NodeExists(node.Address) {
				conf.ClusterNodes = append(conf.ClusterNodes, node.Address)
				joined = true
			} else {
				joined = false
			}
		} else if node.ID != CurrentNode().ID {
			joined = false
			n := <-nl.Find(node.ID)
			n.Update(node)
		}

		config.Save()
		return joined
	}
	return false
}

func AddOrUpdateNodeInfo(node *node.Node) (joined bool) {
	return nodes.AddOrUpdateNodeInfo(config.Get(), node)
}

func (n *ClusterNodes) Find(id string) <-chan *node.Node {
	nc := make(chan *node.Node)
	f := func() {
		n.Lock()
		defer n.Unlock()
		for _, v := range n.Nodes {
			if v.ID == id {
				nc <- v
			}
		}
		close(nc)
	}
	go f()
	return nc
}

func FindNode(id string) <-chan *node.Node {
	return nodes.Find(id)
}

func (n *ClusterNodes) FindCurrentNode() <-chan *node.Node {
	nc := make(chan *node.Node)
	f := func() {
		n.Lock()
		defer n.Unlock()
		for _, v := range n.Nodes {
			if v.Current {
				nc <- v
			}
		}
		close(nc)
	}
	go f()
	return nc
}

func CurrentNode() *node.Node {
	return <-nodes.FindCurrentNode()
}

func (nl *ClusterNodes) GetLessLoadedNode() *node.Node {
	var nodesListSorted []*node.Node
	//nl.Lock()
	nodesListSorted = append(nodesListSorted, nl.AllActive().Nodes...)
	//nl.Unlock()

	sort.Slice(nodesListSorted, func(i, j int) bool {
		return (nodesListSorted[i]).UsedSpace < (nodesListSorted[j]).UsedSpace
	})
	return nodesListSorted[0]

}

func GetLessLoadedNode() *node.Node {
	return nodes.GetLessLoadedNode()
}

func (nl *ClusterNodes) GetLessLoadedNode2() *node.Node {
	var nodesListSorted = AllActive().ToSlice()
	//nl.Lock()

	sort.Slice(nodesListSorted, func(i, j int) bool {
		return (nodesListSorted[i]).GetCurrentJobs() < (nodesListSorted[j]).GetCurrentJobs()
	})
	if nodesListSorted[0].GetCurrentJobs() == 1 {
		FlushNodesCounters()
	}
	nodesListSorted[0].SetCurrentJobs(nodesListSorted[0].GetCurrentJobs() + 1)
	//fmt.Println(nodesListSorted[0])
	return nodesListSorted[0]

}
func GetLessLoadedNode2() *node.Node {
	return nodes.GetLessLoadedNode2()
}

func (nl *ClusterNodes) AllExcept(n *node.Node) []*node.Node {
	var tempList = (*<-New()).Nodes
	for _, v := range nl.Nodes {
		if v.ID != n.ID && v.IsActive {
			tempList = append(tempList, v)
		}
	}
	return tempList
}

func (nl *ClusterNodes) Except(n *node.Node) *ClusterNodes {
	var tempList = <-New()
	for _, v := range nl.Nodes {
		if v.ID != n.ID && v.IsActive {
			tempList.Nodes = append(tempList.Nodes, v)
		}
	}
	return tempList
}

func (nl *ClusterNodes) GetCurrentNode(c *config.Config) *node.Node {
	return <-nl.Find(c.NodeID)
}

func (nl *ClusterNodes) Refresh() {
	var wg sync.WaitGroup
	for _, n := range *config.Nodes() {
		wg.Add(1)
		go func(x config.NodeAddress) {
			defer wg.Done()
			_node, err := nodeInfoExchange(config.Get(), x)
			if err != nil && _node == nil {
				//utils.HandleError(err)
			} else {
				nl.AddOrUpdateNodeInfo(config.Get(), _node)

			}
		}(n)
	}
	wg.Wait()
}

func (nl *ClusterNodes) ToSlice() []*node.Node {
	nl2 := []*node.Node{}
	nl.Lock()
	defer nl.Unlock()
	nl2 = append(nl2, nl.Nodes...)
	return nl2
}

func Refresh() {
	nl := Nodes()
	var wg sync.WaitGroup
	for _, n := range *config.Nodes() {
		wg.Add(1)
		go func(x config.NodeAddress) {
			defer wg.Done()
			_node, err := nodeInfoExchange(config.Get(), x)
			if err != nil && _node == nil {
				//utils.HandleError(err)
			} else {
				nl.AddOrUpdateNodeInfo(config.Get(), _node)

			}
		}(n)
	}
	wg.Wait()
}

func nodeInfoExchange(c *config.Config, address config.NodeAddress) (n *node.Node, err error) {
	var proto = "http"
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		err := json.NewEncoder(w).Encode(CurrentNode())
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

func NodeInfoExchange(c *config.Config, address config.NodeAddress) (*node.Node, error) {
	return nodeInfoExchange(c, address)
}

func (nl ClusterNodes) SendData(data []byte) {
	var wg sync.WaitGroup
	for _, v := range nl.Except(CurrentNode()).ToSlice() {
		wg.Add(1)
		go func(n *node.Node) {
			defer wg.Done()
			err := n.SendData(data)
			if err != nil {
				// todo: error handler
			}
		}(v)
		wg.Wait()
	}
}

func (nl *ClusterNodes) ToJson() <-chan []byte {
	nc := make(chan []byte)
	nl.Lock()
	defer nl.Unlock()
	go func(cl *ClusterNodes) {
		a, err := json.Marshal(cl.Nodes)
		if err != nil {
			// todo: error handler
		}
		nc <- a
		close(nc)
	}(nl)
	return nc
}

func (nl ClusterNodes) FindFile(c *config.Config, fId string, out interface{}) {
	//dc := make(chan []byte)
	//nl.Lock()
	//defer nl.Unlock()
	//go func(n ClusterNodes) {
	for _, v := range AllActive().Except(CurrentNode()).ToSlice() {
		err := v.GetFileJson(fId, &out)
		if err != nil {
			//fmt.Println(out)
		}
	}
	//	close(dc)
	//}(nl)

}

func FindFile(fId string, out interface{}) {
	n := AllActive().Except(CurrentNode()).ToSlice()
	for _, v := range n {
		err := v.GetFileJson(fId, &out)
		if err != nil {
			//fmt.Println(out)
		}
	}

}

func (nl *ClusterNodes) Sort() *ClusterNodes {
	nl2 := <-New()
	nl.Lock()
	nl2.Nodes = append(nl2.Nodes, nl.Nodes...)
	nl.Unlock()

	sort.Slice(nl2.Nodes, func(i, j int) bool {
		return (*nl2.Nodes[i]).GetUsedSpace() < (*nl2.Nodes[j]).GetUsedSpace()
	})
	return nl2
}

func Init() {
	var conf = config.Get()
	var port = config.NodeAddress(strconv.Itoa(conf.NodePort))
	if conf.NodeID == "" {
		id, _ := shortid.Generate()
		conf.NodeID = id
	}
	n := node.NewNode(conf.NodeID, conf.NodeName, "127.0.0.1:"+port, 32212254720, utils.DirSize(conf.DataDir), conf.UseTLS, true)
	nodes.Add(n)
	Refresh()
}

var nodes = &ClusterNodes{}