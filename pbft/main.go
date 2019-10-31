package main

import (
	"github.com/bigpicturelabs/consensusPBFT/pbft/network"
	"os"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"crypto/ecdsa"
	"encoding/pem"
	"crypto/x509"
)

// Hard-coded for test.
var viewID = int64(10000000000)
var nodeTableForTest = []*network.NodeInfo {
	{NodeID: "Apple",  Url: "localhost:1111"},
	{NodeID: "MS",     Url: "localhost:1112"},
	{NodeID: "Google", Url: "localhost:1113"},
	{NodeID: "IBM",    Url: "localhost:1114"},
}
// PrivateKeyDecode
func PrivateKeyDecode(pemEncoded string) *ecdsa.PrivateKey {

	block, _ := pem.Decode([]byte(pemEncoded))
	x509Encoded := block.Bytes
	privateKey, _ := x509.ParseECPrivateKey(x509Encoded)

	return privateKey
}
// PublicKeyDecode
func PublicKeyDecode(pemEncodedPub string) *ecdsa.PublicKey {

	blockPub, _ := pem.Decode([]byte(pemEncodedPub))
	x509EncodedPub := blockPub.Bytes
	genericPublicKey, _ := x509.ParsePKIXPublicKey(x509EncodedPub)
	publicKey := genericPublicKey.(*ecdsa.PublicKey)

	return publicKey
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
	// Make PubKey && setting each for nodeID
	for _, myinfo := range nodeTable {

		pubKeyFile := fmt.Sprintf("keys/%s.pub", myinfo.NodeID)
		pubbytes, _ := ioutil.ReadFile(pubKeyFile)
		decodePubKey := PublicKeyDecode(string(pubbytes))
		myinfo.PubKey = decodePubKey

	}
	// Make NodeID PriveKey
	privKeyFile := fmt.Sprintf("keys/%s.priv", nodeID)
	privbytes, _ := ioutil.ReadFile(privKeyFile)
	decodePrivKey := PrivateKeyDecode(string(privbytes))

	server := network.NewServer(nodeID, nodeTable, viewID, decodePrivKey)

	if server != nil {
		server.Start()
	}
}
