package main

import (
	"os"
	"github.com/bigpicturelabs/consensusPBFT/pbft/network"
)

func main() {
	// Hard-coded for test.
	var viewID = int64(10000000000)

	var nodeTable = []*network.NodeInfo {
		{NodeID: "Apple",  Url: "localhost:1111"},
		{NodeID: "MS",     Url: "localhost:1112"},
		{NodeID: "Google", Url: "localhost:1113"},
		{NodeID: "IBM",    Url: "localhost:1114"},
	}

	nodeID := os.Args[1]
	server := network.NewServer(nodeID, nodeTable, viewID)

	if server != nil {
		server.Start()
	}
}
