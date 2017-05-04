package console

import (
	"bufio"
	"encoding/json"
	"fmt"
	"oakleaf/cluster"
	"oakleaf/storage"
	"oakleaf/utils"
	"os"
	"strings"
	"time"
)

func Worker() {
	time.Sleep(1 * time.Second)
	reader := bufio.NewReader(os.Stdin)
	for {
		var err error
		fmt.Print("#>: ")
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		switch text {
		case "":
			continue
		case "files":
			filesJson, _ := json.Marshal(storage.Files.List())
			fmt.Println(utils.JsonPrettyPrint(string(filesJson)))
		case "nodes":
			nodesJson, _ := json.Marshal(cluster.Nodes.All().Nodes)
			fmt.Println(utils.JsonPrettyPrint(string(nodesJson)))
			//fmt.Println("All nodes started")
		case "exit":
			fmt.Println("Exiting....")
			os.Exit(0)
		case "download":

			//testing find
		default:
			//fmt.Println("Error: no command found: " + text)
			//var f = Files.Find(text)
			//fileJson, _ := json.Marshal(f)
			//fmt.Println(jsonPrettyPrint(string(fileJson)))
			//	err = Files.Find(text).Download()
		}
		if err != nil {
			utils.HandleError(err)
		}
	}
}
