package main

import (
	"encoding/json"
	"fmt"
	"network"
	"os"
)

// Hard-coded for test.
var viewID = int64(10000000000)
var nodeTableForTest = []*network.NodeInfo{
	{NodeID: "Apple", Url: "localhost:1111"},
	{NodeID: "MS", Url: "localhost:1112"},
	{NodeID: "Google", Url: "localhost:1113"},
	{NodeID: "IBM", Url: "localhost:1114"},
	// {NodeID: "SAMSUNG", Url: "localhost:1115"},
	// {NodeID: "LG", Url: "localhost:1116"},
	// {NodeID: "HanYang", Url: "localhost:1117"},
	// {NodeID: "HYUNDAI", Url: "localhost:1118"},
	// {NodeID: "SK", Url: "localhost:1119"},
	// {NodeID: "KT", Url: "localhost:1120"},

	// {NodeID: "MS1", Url: "localhost:1121"},
	// {NodeID: "Google1", Url: "localhost:1122"},
	// {NodeID: "IBM1", Url: "localhost:1123"},
	// {NodeID: "SAMSUNG1", Url: "localhost:1124"},
	// {NodeID: "LG1", Url: "localhost:1125"},
	// {NodeID: "HanYang1", Url: "localhost:1126"},
	// {NodeID: "HYUNDAI1", Url: "localhost:1127"},
	// {NodeID: "SK1", Url: "localhost:1128"},
	// {NodeID: "KT1", Url: "localhost:1129"},
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
