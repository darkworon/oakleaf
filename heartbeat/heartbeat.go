package heartbeat

import (
	"oakleaf/cluster"
	"oakleaf/cluster/node"
	"oakleaf/config"
	"oakleaf/storage"
	"oakleaf/utils"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

func Start(p time.Duration, c *config.Config) {
	go nodeInfoWorker()
	go worker(p, c)
}

func worker(p time.Duration, c *config.Config) {
	time.Sleep(p)
	// update UsedSpace
	for !config.ShuttingDown {
		//storage.Save()
		var wg sync.WaitGroup
		for _, x := range cluster.Nodes().Except(cluster.CurrentNode()).ToSlice() {
			wg.Add(1)
			//	if Nodes.FindNode(n) == nil {
			go func(n *node.Node) {
				defer wg.Done()
				//fmt.Println(n.Address)
				_node, err := cluster.NodeInfoExchange(c, n.Address)
				//fmt.Println("33333")
				if err != nil {
					//	HandleError(err)
					if n.IsActive {
						log.Warnf("Node %s -> inactive", n.Address)
						n.IsActive = false
						//fmt.Println(n.IsActive)
					}
				} else if _node != nil {
					cluster.AddOrUpdateNodeInfo(_node)
					//	fmt.Println(_node)
				}
			}(x)
		}
		wg.Wait()
		time.Sleep(p)
	}
}

func nodeInfoWorker() {
	for {
		cluster.CurrentNode().SetUsedSpace(utils.DirSize(config.Get().DataDir))
		cluster.CurrentNode().SetPartsCount(storage.Count())
		time.Sleep(500 * time.Millisecond)
	}
}
