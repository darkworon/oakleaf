package balancing

import "time"

func Worker(period time.Duration) {
	go func() {
		for {
			Rebalance()
			time.Sleep(period)
		}
	}()

}
