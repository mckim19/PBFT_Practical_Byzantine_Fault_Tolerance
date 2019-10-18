package main

import (
	"github.com/bigpicturelabs/consensusPBFT/pbft/network"
	"os"
	"encoding/json"
	"fmt"
)

// Hard-coded for test.
var viewID = int64(10000000000)
var nodeTableForTest = []*network.NodeInfo {
	{NodeID: "Apple",  Url: "localhost:1111"},
	{NodeID: "MS",     Url: "localhost:1112"},
	{NodeID: "Google", Url: "localhost:1113"},
	{NodeID: "IBM",    Url: "localhost:1114"},
}

func main() {
	var nodeTable []*network.NodeInfo

	if len(os.Args) < 2 {
		fmt.Println("Usage:", os.Args[0], "<nodeID> [node.list]")
		return
	}

	nodeID := os.Args[1]
	if len(os.Args) == 2 {
		fmt.Println("Node list are not specified")
		fmt.Println("Embedded list is used for test")
		nodeTable = nodeTableForTest
	} else {
		nodeListFile := os.Args[2]
		jsonFile, err := os.Open(nodeListFile)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer jsonFile.Close()

		err = json.NewDecoder(jsonFile).Decode(&nodeTable)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	server := network.NewServer(nodeID, nodeTable, viewID)

	if server != nil {
		server.Start()
	}
}
