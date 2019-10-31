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
	"log"
)

// Hard-coded for test.
var viewID = int64(10000000000)
var nodeTableForTest = []*network.NodeInfo {
	{NodeID: "Apple",  Url: "localhost:1111"},
	{NodeID: "MS",     Url: "localhost:1112"},
	{NodeID: "Google", Url: "localhost:1113"},
	{NodeID: "IBM",    Url: "localhost:1114"},
}

func PrivateKeyDecode(pemEncoded []byte) *ecdsa.PrivateKey {
	blockPriv, _ := pem.Decode(pemEncoded)
	x509Encoded := blockPriv.Bytes
	privateKey, _ := x509.ParseECPrivateKey(x509Encoded)

	return privateKey
}

func PublicKeyDecode(pemEncoded []byte) *ecdsa.PublicKey {
	blockPub, _ := pem.Decode(pemEncoded)
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
		AssertError(err)
		defer jsonFile.Close()

		err = json.NewDecoder(jsonFile).Decode(&nodeTable)
		AssertError(err)
	}

	// Load public key for each node.
	for _, nodeInfo := range nodeTable {
		pubKeyFile := fmt.Sprintf("keys/%s.pub", nodeInfo.NodeID)
		pubBytes, err := ioutil.ReadFile(pubKeyFile)
		AssertError(err)

		decodePubKey := PublicKeyDecode(pubBytes)
		nodeInfo.PubKey = decodePubKey
	}

	// Make NodeID PriveKey
	privKeyFile := fmt.Sprintf("keys/%s.priv", nodeID)
	privbytes, err := ioutil.ReadFile(privKeyFile)
	AssertError(err)
	decodePrivKey := PrivateKeyDecode(privbytes)

	server := network.NewServer(nodeID, nodeTable, viewID, decodePrivKey)

	if server != nil {
		server.Start()
	}
}

func AssertError(err error) {
	if err == nil {
		return
	}

	log.Println(err)
	os.Exit(1)
}
