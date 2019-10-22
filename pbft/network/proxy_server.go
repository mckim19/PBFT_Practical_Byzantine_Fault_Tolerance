// TODO: secure connection such as HTTPS, or manual implementation
// from Section 5.2.2 Key Exchanges on TOCS.
package network

import (
	"github.com/gorilla/websocket"
	"net/http"
	"net/url"

	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
	//"encoding/json"
	"log"
	"time"
)

type Server struct {
	url string
	node *Node
}

func NewServer(nodeID string, nodeTable []*NodeInfo, viewID int64) *Server {
	nodeIdx := int(-1)
	for idx, nodeInfo := range nodeTable {
		if nodeInfo.NodeID == nodeID {
			nodeIdx = idx
			break
		}
	}

	if nodeIdx == -1 {
		log.Printf("Node '%s' does not exist!\n", nodeID)
		return nil
	}

	node := NewNode(nodeID, nodeTable, viewID)
	server := &Server{nodeTable[nodeIdx].Url, node}

	hub := NewHub()
	go hub.run()
	handler := func(w http.ResponseWriter, r *http.Request) {
		ServeWs(hub, w, r)
	}
	http.HandleFunc("/req", handler)
	http.HandleFunc("/preprepare", handler)
	http.HandleFunc("/prepare", handler)
	http.HandleFunc("/commit", handler)
	http.HandleFunc("/reply", handler)

	return server
}

func (server *Server) Start() {
	log.Printf("Server will be started at %s...\n", server.url)

	go server.DialOtherNodes()

	if err := http.ListenAndServe(server.url, nil); err != nil {
		log.Println(err)
		return
	}
}

func (server *Server) DialOtherNodes() {
	// Sleep until all nodes perform ListenAndServ().
	time.Sleep(time.Second * 2)

	var thisNodeInfo *NodeInfo

	for _, nodeInfo := range server.node.NodeTable {
		u := url.URL{Scheme: "ws", Host: nodeInfo.Url, Path: "/req"}
		log.Printf("connecting to %s", u.String())

		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Fatal("dial:", err)
		}
		defer c.Close()

		if nodeInfo.NodeID != server.node.NodeID {
			go server.receiveLoop(c, nodeInfo)
		} else {
			thisNodeInfo = nodeInfo
		}
	}

	go server.sendLoop(thisNodeInfo)

	time.Sleep(time.Second * 1000000)
}

func (server *Server) receiveLoop(c *websocket.Conn, nodeInfo *NodeInfo) {
	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			return
		}
		log.Printf("%s recv: %s", server.node.NodeID, consensus.Hash(message))
	}
}

func (server *Server) sendLoop(nodeInfo *NodeInfo) {
	ticker := time.NewTicker(time.Millisecond * 200)
	defer ticker.Stop()

	dummy := make([]byte, 1500000)
	for i := range dummy {
		dummy[i] = 'A'
	}
	dummy[len(dummy) - 1] = 0

	u := url.URL{Scheme: "ws", Host: nodeInfo.Url, Path: "/req"}
	log.Printf("connecting to %s", u.String())

	for {
		select {
		case <-ticker.C:
			//dummyReq := "{\"operation\": \"Op1\", \"clientID\": \"Client1\", \"data\": \"JJWEJPQOWJE\", \"timestamp\": 190283901}"
			c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
			if err != nil {
				log.Fatal("dial:", err)
			}

			err = c.WriteMessage(websocket.TextMessage, dummy)
			if err != nil {
				log.Println("write:", err)
				return
			}
			c.Close()
		}
	}
}
