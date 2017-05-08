package client

import (
	//"encoding/json"
	//"oakleaf/cluster"
	"net/http"
	"oakleaf/config"
	"time"
	//"oakleaf/files"

	"io"
	//"io/ioutil"
	//"fmt"
)

//type Cluster *cluster.NodesList
type Config *config.Config

func JoinCluster(nodeAddr string) (bool, error) {
	return true, nil
}

func New() *http.Client {
	timeout := time.Duration(2 * time.Second)
	c := &http.Client{
		Timeout: timeout,
		/*Transport: &http.Transport{
			//MaxIdleConns:    10,
			//IdleConnTimeout: 30 * time.Second,
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},*/
	}
	return c
}

func Get(url string) (*http.Response, error) {
	//fmt.Println("Sending get to", url)
	return New().Get(url)
}

func Post(url string, contentType string, body io.Reader) (*http.Response, error) {
	//fmt.Println("Sending post to", url)
	return New().Post(url, contentType, body)
}

func Head(url string) (*http.Response, error) {
	//fmt.Println("Sending post to", url)
	return New().Head(url)
}
