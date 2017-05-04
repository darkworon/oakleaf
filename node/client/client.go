package client

import (
	//"encoding/json"
	//"oakleaf/cluster"
	"oakleaf/config"
	//"oakleaf/file"
	//"oakleaf/node"
	"net/http"
	//"oakleaf/utils"
	"bytes"
	"encoding/json"
	"fmt"
	//"io/ioutil"
)

//type Cluster *cluster.NodesList
type Config *config.Config

func JoinCluster(nodeAddr string) (bool, error) {
	return true, nil
}

func GetFileJson(address string, fileId string, out *interface{}) error {
	resp, _ := http.Get(fmt.Sprintf("http://%s/file/info/%s", address, fileId))
	defer resp.Body.Close()
	err := json.NewDecoder(resp.Body).Decode(&out)
	return err
}

/*
func GetFileJson(address string, fileId string) []byte {
	var f *file.File = new(file.File)
	for _, v := range address {
		//fmt.Printf("Trying to get file %s info from node %s...\n", fileId, address)
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

func SendFileInfo(addr string, data []byte) error {
	resp, err := http.Post(fmt.Sprintf("http://%s/file/info", addr), "application/json", bytes.NewBuffer(data))
	defer resp.Body.Close()
	return err

}

/*

func getFileInfoFromAllNodes(id string) *file.File {
	var f *file.File = new(file.File)
	cluster.Nodes.Lock()
	defer cluster.Nodes.Unlock()
	for _, v := range Nodes {
		fmt.Printf("Trying to get file %s info from node %s...\n", id, v.Address)
		resp, err := http.Get(fmt.Sprintf("http://%s/file/info/%s", v.Address, id))
		if err != nil {
			HandleError(err)
			continue
		}
		defer resp.Body.Close()
		err = json.NewDecoder(resp.Body).Decode(&f)
		if err != nil {
			// if cant unmarshall - thinking we got no file info
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
