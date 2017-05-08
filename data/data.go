package data

import (
	"net"
	"oakleaf/config"
)

type content []byte

type Packet struct {
	Address config.NodeAddress
	URL     string
	Content content
}

func GetIP() config.NodeAddress {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return config.NodeAddress(ipnet.IP.String())
			}
		}
	}
	return ""
}
