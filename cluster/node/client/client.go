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

func New(timeout time.Duration) *http.Client {
	c := &http.Client{
		Timeout: timeout,
		//Transport: &http.Transport{
		//	MaxIdleConns:          10,
		//	IdleConnTimeout:       30 * time.Second,
		//ResponseHeaderTimeout: 5 * time.Second,
		//TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		//},
	}
	return c
}

func Get(url string, timeout time.Duration) (*http.Response, error) {
	//fmt.Println("Sending get to", url)
	return New(timeout).Get(url)
}

func Post(url string, contentType string, body io.Reader, timeout time.Duration) (*http.Response, error) {
	//fmt.Println("Sending post to", url)
	return New(timeout).Post(url, contentType, body)
}

func Head(url string, timeout time.Duration) (*http.Response, error) {
	//fmt.Println("Sending post to", url)
	return New(timeout).Head(url)
}
