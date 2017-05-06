package data

import "oakleaf/config"

type content []byte

type Packet struct {
	Address config.NodeAddress
	URL     string
	Content content
}
