package console

import (
	"bufio"
	"encoding/json"
	"fmt"
	"oakleaf/cluster"
	"oakleaf/utils"
	"os"
	"strings"
	"oakleaf/files"
	"time"
	"github.com/darkworon/oakleaf/cluster/balancing"
	"github.com/darkworon/oakleaf/storage"
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
			filesJson, _ := json.Marshal(files.All().List())
			fmt.Println(utils.JsonPrettyPrint(string(filesJson)))
		case "parts":
			fmt.Println(utils.JsonPrettyPrint(string(storage.All().Json())))
		case "nodes":
			nodesJson, _ := json.Marshal(cluster.Nodes().ToSlice())
			fmt.Println(utils.JsonPrettyPrint(string(nodesJson)))
			//fmt.Println("All nodes started")
		case "exit":
			fmt.Println("Exiting....")
			os.Exit(0)
		case "save":
			storage.Save()
			//testing find
		case "rebalance":
			go func() {
				err := balancing.Rebalance()
				if err != nil {
					fmt.Println(err)
				}
			}()
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
