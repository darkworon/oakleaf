package config

type NodeAddress string
type AddressList []NodeAddress

func (c *AddressList) IsExists(n NodeAddress) bool {
	for _, v := range *c {
		if v == n {
			return true
		}
	}
	return false
}

func (c *Config) Nodes() (*AddressList) {
	a := &AddressList{}
	b := &Get().ClusterNodes
	*a = *b
	return &Get().ClusterNodes
}

func Nodes() (*AddressList) {
	return &Get().ClusterNodes
}
