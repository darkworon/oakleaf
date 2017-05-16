package heartbeat

import (
	"oakleaf/cluster"
	"oakleaf/cluster/node"
	"oakleaf/config"
	"oakleaf/storage"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

func Start(p time.Duration, c *config.Config) {
	go nodeInfoWorker()
	go worker(p, c)
}

func worker(p time.Duration, c *config.Config) {
	for {
		var wg sync.WaitGroup
		nodes := cluster.Nodes().Except(cluster.CurrentNode())
		//	list := strings.Join(nodes.AddressListStr(), ", ")
		//	log.Debugf("Sending info exchange to nodes %s", list) //%s", strings.Join(nodes.AddressListStr(), ", "))
		for _, x := range nodes.ToSlice() {
			wg.Add(1)
			go func(n *node.Node) {
				defer wg.Done()
				_node, err := cluster.NodeInfoExchange(c, n.Address)
				if err != nil {
					if n.IsActive {
						log.Debug(err)
						log.Warnf("Node %s -> inactive", n.Address)
						n.IsActive = false
					}
				} else if _node != nil {
					cluster.AddOrUpdateNodeInfo(_node)
				}
			}(x)
		}
		wg.Wait()
		time.Sleep(p)
	}
}

func nodeInfoWorker() {
	for {
		cluster.CurrentNode().SetUsedSpace(storage.TotalSize())
		cluster.CurrentNode().SetPartsCount(storage.Count())
		time.Sleep(500 * time.Millisecond)
	}
}
