package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"oakleaf/cluster/node"
	"oakleaf/cluster/node/client"
	"oakleaf/config"
	"oakleaf/data"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/darkworon/oakleaf/utils"
	"github.com/ventu-io/go-shortid"
)

type ClusterNodes struct {
	sync.RWMutex `json:"-"`
	Nodes        []*node.Node `json:"nodes"`
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
	//log.Debugf("Locked on %s", whereami.WhereAmI())
	defer nl.Unlock()
	// defer //log.Debugf("Unlocked on %s", whereami.WhereAmI())
	nl2.Nodes = append(nl2.Nodes, nl.Nodes...)
	return nl2
}

func (nl *ClusterNodes) SortBy(field string) *ClusterNodes {
	nl2 := <-New()
	nl.Lock()
	//log.Debugf("Locked on %s", whereami.WhereAmI())
	defer nl.Unlock()
	// defer //log.Debugf("Unlocked on %s", whereami.WhereAmI())
	nl2.Nodes = append(nl2.Nodes, nl.Nodes...)
	sort.Slice(nl2.Nodes, func(i, j int) bool {
		r1 := reflect.ValueOf(nl2.Nodes[i]).Elem()
		r2 := reflect.ValueOf(nl2.Nodes[j]).Elem()
		f1 := reflect.Indirect(r1).FieldByName(field)
		f2 := reflect.Indirect(r2).FieldByName(field)
		switch f1.Kind() {
		case reflect.Int64:
			//	fmt.Println("Sorting int")
			return f1.Int() < f2.Int()
		case reflect.String:
			//	fmt.Println("Sorting string")
			return f1.String() < f2.String()
		default:
			//fmt.Println("WTF :(")
			return false
		}

	})
	return nl2
}

func (nl *ClusterNodes) AllActive() *ClusterNodes {
	nl2 := <-New()
	nl.Lock()
	//log.Debugf("Locked on %s", whereami.WhereAmI())
	defer nl.Unlock()
	// defer //log.Debugf("Unlocked on %s", whereami.WhereAmI())
	for _, x := range nl.Nodes {
		if x.IsActive {
			nl2.Nodes = append(nl2.Nodes, x)
		}
	}
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
	//log.Debugf("Locked on %s", whereami.WhereAmI())
	defer nl.Unlock()
	// defer //log.Debugf("Unlocked on %s", whereami.WhereAmI())
	if _n == nil {
		nl.Nodes = append(nl.Nodes, n)
	}

}

func (nl *ClusterNodes) Count() int {
	nl.Lock()
	//log.Debugf("Locked on %s", whereami.WhereAmI())
	defer nl.Unlock()
	// defer //log.Debugf("Unlocked on %s", whereami.WhereAmI())
	return len(nl.Nodes)
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
		//fmt.Println("AddOrUpdateNodeInfo2")
		if !nl.IsNodeExists(node) {
			nl.Add(node)
			//	fmt.Println("AddOrUpdateNodeInfo3")
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
	n.Lock()
	defer n.Unlock()
	// defer //log.Debugf("Unlocked on %s", whereami.WhereAmI())
	//log.Debugf("Locked on %s", whereami.WhereAmI())
	f := func() {
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
	n.Lock()
	defer n.Unlock()
	//log.Debugf("Locked on %s", whereami.WhereAmI())
	f := func() {
		// defer //log.Debugf("Unlocked on %s", whereami.WhereAmI())
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

func GetMostLoadedNode() *node.Node {
	nl := nodes
	var nodesListSorted []*node.Node
	//nl.Lock()
	nodesListSorted = append(nodesListSorted, nl.AllActive().ToSlice()...)
	//nl.Unlock()

	sort.Slice(nodesListSorted, func(i, j int) bool {
		return (nodesListSorted[i]).UsedSpace > (nodesListSorted[j]).UsedSpace
	})
	return nodesListSorted[0]

}

func (nl *ClusterNodes) GetLessLoadedNode() *node.Node {
	var nodesListSorted = []*node.Node{}
	//nl.Lock()
	nodesListSorted = append(nodesListSorted, nl.AllActive().ToSlice()...)
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
		if v.ID != n.ID {
			tempList = append(tempList, v)
		}
	}
	return tempList
}

func (nl *ClusterNodes) AllActiveExcept(n *node.Node) []*node.Node {
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
		if v.ID != n.ID {
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
	//log.Debugf("Locked on %s", whereami.WhereAmI())
	defer nl.Unlock()
	// defer //log.Debugf("Unlocked on %s", whereami.WhereAmI())
	nl2 = append(nl2, nl.Nodes...)
	return nl2
}

func Refresh() {
	//fmt.Println("Refreshing: started")
	var wg sync.WaitGroup
	for _, n := range *config.Nodes() {
		wg.Add(1)
		go func(x config.NodeAddress) {
			defer wg.Done()
			_node, err := nodeInfoExchange(config.Get(), x)
			if err != nil || _node.IsEmpty() {
				//utils.HandleError(err)
			} else {
				nodes.AddOrUpdateNodeInfo(config.Get(), _node)
				//fmt.Println("AZAZA",_node)
			}
		}(n)
	}
	wg.Wait()
	//fmt.Println("Refreshing: ended")
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
	resp, err := client.Post(fmt.Sprintf("%s://%s/api/node/info", proto, address), "application/json; charset=utf-8", r, 3*time.Second)
	if resp != nil {
		defer resp.Body.Close()
		//defer fmt.Println("Closing connection...")
	}
	if err != nil {
		return nil, err
	}
	n = &node.Node{}
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
				utils.HandleError(err)
			}
		}(v)
		wg.Wait()
	}
}

func (nl *ClusterNodes) ToJson() <-chan []byte {
	nc := make(chan []byte)
	nl.Lock()
	//log.Debugf("Locked on %s", whereami.WhereAmI())
	defer nl.Unlock()
	// defer //log.Debugf("Unlocked on %s", whereami.WhereAmI())
	go func(cl *ClusterNodes) {
		a, err := json.Marshal(cl.Nodes)
		if err != nil {
			log.Error(err)
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
	//log.Debugf("Locked on %s", whereami.WhereAmI())
	nl2.Nodes = append(nl2.Nodes, nl.Nodes...)
	nl.Unlock()
	//log.Debugf("Unlocked on %s", whereami.WhereAmI())

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
	n := node.NewNode(conf.NodeID, conf.NodeName, data.GetIP()+":"+port, 32212254720, utils.DirSize(conf.DataDir), conf.UseTLS, true)
	n.IsActive = false
	nodes.Add(n)
	//fmt.Println("Started refreshing nodes list...")
	Refresh()
	n.IsActive = true
	n.SetStatus(node.StateFullyActive)
	//fmt.Println("Refreshing done...")
	//fmt.Println(nodes.ToSlice()[0], nodes.ToSlice()[1])
}

var nodes = &ClusterNodes{}
