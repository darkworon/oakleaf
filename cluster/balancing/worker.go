package balancing

import (
	"time"
	"oakleaf/config"
)

// Worker
func Worker(period time.Duration) {
	go func() {
		for !config.ShuttingDown {
			time.Sleep(period)
			Rebalance()
		}
	}()

}
