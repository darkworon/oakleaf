package heartbeat

import (
	"oakleaf/cluster"
	"oakleaf/cluster/node"
	"oakleaf/config"
	//	"oakleaf/storage"
	"fmt"
	"sync"
	"time"
)

var nodes = cluster.Nodes

func Worker(p time.Duration, c *config.Config) {
	go worker(p, c)
}

func worker(p time.Duration, c *config.Config) {
	time.Sleep(p)
	for {
		var wg sync.WaitGroup
		for _, x := range cluster.Nodes.AllExcept(cluster.GetCurrentNode(c)) {
			//fmt.Println()
			wg.Add(1)
			//	if Nodes.FindNode(n) == nil {
			go func(n *node.Node, cf *config.Config) {
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
					nodes.AddOrUpdateNodeInfo(c, _node)
					//fmt.Println("555555")
				}
			}(x, c)
		}
		wg.Wait()
		time.Sleep(p)
	}
}
