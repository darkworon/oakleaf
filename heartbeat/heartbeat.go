package heartbeat

import (
	"oakleaf/cluster"
	"oakleaf/cluster/node"
	"oakleaf/config"
	"fmt"
	"oakleaf/utils"
	"sync"
	"time"
	"oakleaf/storage"
)


func Start(p time.Duration, c *config.Config) {
	go worker(p, c)
}

func worker(p time.Duration, c *config.Config) {
	time.Sleep(p)
	// update UsedSpace
	for {
		cluster.CurrentNode().SetUsedSpace(utils.DirSize(c.DataDir))
		cluster.CurrentNode().SetPartsCount(storage.Count())
		//storage.Save()
		var wg sync.WaitGroup
		for _, x := range cluster.AllActive().Except(cluster.CurrentNode()).ToSlice() {
			wg.Add(1)
			//	if Nodes.FindNode(n) == nil {
			go func(n *node.Node) {
				defer wg.Done()
				//	fmt.Println(n)
				_node, err := cluster.NodeInfoExchange(c, n.Address)
				//fmt.Println("33333")
				if err != nil {
					//	HandleError(err)
					if n.IsActive {
						defer fmt.Printf("[CLUSTER] Node %s -> not active.\n", n.Address)
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
