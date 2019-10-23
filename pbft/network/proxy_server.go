// TODO: secure connection such as HTTPS, or manual implementation
// from Section 5.2.2 Key Exchanges on TOCS.
package network

import (
	"github.com/gorilla/websocket"
	"net/http"
	"net/url"

	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
	"encoding/json"
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

	for _, nodeInfo := range server.node.NodeTable {
		u := url.URL{Scheme: "ws", Host: nodeInfo.Url, Path: "/req"}
		log.Printf("connecting to %s", u.String())

		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Fatal("dial:", err)
		}
		defer c.Close()

		go server.receiveLoop(c, nodeInfo)
	}

	go server.sendDummyMsg()

	// Wait.
	select {}
}

func (server *Server) receiveLoop(c *websocket.Conn, nodeInfo *NodeInfo) {
	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			return
		}
		log.Printf("%s recv: %s", server.node.NodeID, consensus.Hash(message))

		var msg consensus.RequestMsg
		err = json.Unmarshal(message, &msg)
		if err != nil {
			log.Println(err)
			continue
		}
		server.node.MsgEntrance <- &msg
	}
}

func (server *Server) sendDummyMsg() {
	// Send dummy message from primary node.
	// TODO: send message from the current (changed) primary node.
	primaryNode := server.node.View.Primary
	if primaryNode.NodeID != server.node.NodeID {
		return
	}

	// Set periodic send signal.
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	// Create a dummy message.
	dummy := dummyMsg(1500000, "Op1", "Client1", time.Now().UnixNano())

	u := url.URL{Scheme: "ws", Host: primaryNode.Url, Path: "/req"}
	log.Printf("connecting to %s", u.String())

	for {
		select {
		case <-ticker.C:
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

func dummyMsg(dummySize int, operation string, clientID string, timestamp int64) []byte {
	var msg consensus.RequestMsg
	msg.Operation = operation
	msg.ClientID = clientID
	msg.Timestamp = timestamp

	data := make([]byte, dummySize)
	for i := range data {
		data[i] = 'A'
	}
	data[dummySize - 1] = 0

	msg.Data = string(data)

	// {"operation": "Op1", "clientID": "Client1", "data": "JJWEJPQOWJE", "timestamp": 190283901}
	jsonMsg, err := json.Marshal(&msg)
	if err != nil {
		log.Println(err)
		return nil
	}

	return []byte(jsonMsg)
}
