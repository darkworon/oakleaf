package balancing

import "time"

func Worker(period time.Duration) {
	go func() {
		for {
			time.Sleep(period)
			Rebalance()
		}
	}()

}
