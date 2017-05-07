package client

import (
	//"encoding/json"
	//"oakleaf/cluster"
	"net/http"
	"oakleaf/config"
	//"oakleaf/files"
	"crypto/tls"
	"io"
	//"io/ioutil"
	//"fmt"
)

//type Cluster *cluster.NodesList
type Config *config.Config

func JoinCluster(nodeAddr string) (bool, error) {
	return true, nil
}

func new() *http.Client {
	tr := &http.Transport{
		//MaxIdleConns:    10,
		//IdleConnTimeout: 30 * time.Second,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	return &http.Client{Transport: tr}
}

func Get(url string) (*http.Response, error) {
	//fmt.Println("Sending get to", url)
	return new().Get(url)
}

func Post(url string, contentType string, body io.Reader) (*http.Response, error) {
	//fmt.Println("Sending post to", url)
	return new().Post(url, contentType, body)
}

func Head(url string) (*http.Response, error) {
	//fmt.Println("Sending post to", url)
	return new().Head(url)
}

/*
func GetFileJson(address string, fileId string) []byte {
	var f *files.File = new(files.File)
	for _, v := range address {
		//fmt.Printf("Trying to get files %s info from node %s...\n", fileId, address)
		resp, err := http.Get(fmt.Sprintf("http://%s/file/info/%s", address, fileId))
		if err != nil {
			utils.HandleError(err)
			continue
		}
		defer resp.Body.Close()

		if err != nil {
			utils.HandleError(err)
			continue
		}
		if f != nil {
			//fmt.Println("Found it! Adding to our list and sending back")
			break
		}
	}
	return f
}*/

/*

func getFileInfoFromAllNodes(id string) *files.File {
	var f *files.File = new(files.File)
	cluster.Nodes.Lock()
	defer cluster.Nodes.Unlock()
	for _, v := range Nodes {
		fmt.Printf("Trying to get files %s info from node %s...\n", id, v.Address)
		resp, err := http.Get(fmt.Sprintf("http://%s/file/info/%s", v.Address, id))
		if err != nil {
			HandleError(err)
			continue
		}
		defer resp.Body.Close()
		err = json.NewDecoder(resp.Body).Decode(&f)
		if err != nil {
			// if cant unmarshall - thinking we got no files info
			//HandleError(err)
			continue
		}
		if f != nil {
			//fmt.Println(f)
			fmt.Println("Found it! Adding to our list and sending back")
			//Files.List = append(Files.List, f)
			Files.Add(f)
			//go updateIndexFiles() - пока хз
			break
		}
	}
	return f
}*/
