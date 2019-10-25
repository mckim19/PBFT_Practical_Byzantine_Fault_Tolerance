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

	node := NewNode(nodeTable[nodeIdx], nodeTable, viewID)
	server := &Server{nodeTable[nodeIdx].Url, node}

	// Normal case.
	server.setRoute("/req")
	server.setRoute("/preprepare")
	server.setRoute("/prepare")
	server.setRoute("/commit")
	server.setRoute("/reply")

	// View change.
	server.setRoute("/checkpoint")
	server.setRoute("/viewchange")
	server.setRoute("/newview")

	return server
}

func (server *Server) setRoute(path string) {
	hub := NewHub()
	handler := func(w http.ResponseWriter, r *http.Request) {
		ServeWs(hub, w, r)
	}
	http.HandleFunc(path, handler)

	go hub.run()
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
	time.Sleep(time.Second * 3)

	// Normal case.
	var cReq = make(map[string]*websocket.Conn)
	var cPrePrepare = make(map[string]*websocket.Conn)
	var cPrepare = make(map[string]*websocket.Conn)
	var cCommit = make(map[string]*websocket.Conn)
	var cReply = make(map[string]*websocket.Conn)

	// View change.
	var cCheckPoint = make(map[string]*websocket.Conn)
	var cViewChange = make(map[string]*websocket.Conn)
	var cNewView = make(map[string]*websocket.Conn)

	for _, nodeInfo := range server.node.NodeTable {
		cReq[nodeInfo.NodeID] = server.setReceiveLoop("/req", nodeInfo)
		cPrePrepare[nodeInfo.NodeID] = server.setReceiveLoop("/preprepare", nodeInfo)
		cPrepare[nodeInfo.NodeID] = server.setReceiveLoop("/prepare", nodeInfo)
		cCommit[nodeInfo.NodeID] = server.setReceiveLoop("/commit", nodeInfo)
		cReply[nodeInfo.NodeID] = server.setReceiveLoop("/reply", nodeInfo)

		cCheckPoint[nodeInfo.NodeID] = server.setReceiveLoop("/checkpoint", nodeInfo)
		cViewChange[nodeInfo.NodeID] = server.setReceiveLoop("/viewchange", nodeInfo)
		cNewView[nodeInfo.NodeID] = server.setReceiveLoop("/newview", nodeInfo)
	}

	go server.sendDummyMsg()

	// Wait.
	select {}

	//defer c.Close()
}

func (server *Server) setReceiveLoop(path string, nodeInfo *NodeInfo) *websocket.Conn {
	u := url.URL{Scheme: "ws", Host: nodeInfo.Url, Path: path}
	log.Printf("connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
		return nil
	}

	go server.receiveLoop(c, path, nodeInfo)

	return c
}

func (server *Server) receiveLoop(c *websocket.Conn, path string, nodeInfo *NodeInfo) {
	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			return
		}
		//log.Printf("%s recv: %s", server.node.NodeID, consensus.Hash(message))

		switch path {
		case "/req":
			var msg consensus.RequestMsg
			err = json.Unmarshal(message, &msg)
			if err != nil {
				log.Println(err)
				continue
			}
			server.node.MsgEntrance <- &msg
		case "/preprepare":
			var msg consensus.PrePrepareMsg
			err = json.Unmarshal(message, &msg)
			if err != nil {
				log.Println(err)
				continue
			}
			server.node.MsgEntrance <- &msg
		case "/prepare":
			var msg consensus.VoteMsg
			err = json.Unmarshal(message, &msg)
			if err != nil {
				log.Println(err)
				continue
			}
			server.node.MsgEntrance <- &msg
		case "/commit":
			var msg consensus.VoteMsg
			err = json.Unmarshal(message, &msg)
			if err != nil {
				log.Println(err)
				continue
			}
			server.node.MsgEntrance <- &msg
		case "/reply":
			var msg consensus.ReplyMsg
			err = json.Unmarshal(message, &msg)
			if err != nil {
				log.Println(err)
				continue
			}
			server.node.MsgEntrance <- &msg
		case "/checkpoint":
			var msg consensus.CheckPointMsg
			err = json.Unmarshal(message, &msg)
			if err != nil {
				log.Println(err)
				continue
			}
			server.node.MsgEntrance <- &msg
		case "/viewchange":
			var msg consensus.ViewChangeMsg
			err = json.Unmarshal(message, &msg)
			if err != nil {
				log.Println(err)
				continue
			}
			server.node.MsgEntrance <- &msg
		case "/newview":
			var msg consensus.NewViewMsg
			err = json.Unmarshal(message, &msg)
			if err != nil {
				log.Println(err)
				continue
			}
			server.node.MsgEntrance <- &msg
		}
	}
}

func (server *Server) sendDummyMsg() {
	// Send dummy message from primary node.
	// TODO: send message from the current (changed) primary node.
	primaryNode := server.node.View.Primary
	if primaryNode.NodeID != server.node.MyInfo.NodeID {
		return
	}

	// Set periodic send signal.
	ticker := time.NewTicker(time.Millisecond * 500)
	defer ticker.Stop()

	// Create a dummy data.
	data := make([]byte, 1 << 20)
	for i := range data {
		data[i] = 'A'
	}
	data[len(data) - 1] = 0

	u := url.URL{Scheme: "ws", Host: primaryNode.Url, Path: "/req"}
	log.Printf("connecting to %s", u.String())

	for {
		select {
		case <-ticker.C:
			c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
			if err != nil {
				log.Fatal("dial:", err)
			}

			// Create a dummy message and send it.
			dummy := dummyMsg("Op1", "Client1", data)
			err = c.WriteMessage(websocket.TextMessage, dummy)
			if err != nil {
				log.Println("write:", err)
				return
			}
			c.Close()
		}
	}
}

func broadcast(errCh chan<- error, url string, msg []byte) {
	url = "ws://" + url // Fix using url.URL{}
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		errCh <- err
		return
	}

	err = c.WriteMessage(websocket.TextMessage, msg)
	if err != nil {
		errCh <- err
		return
	}
	defer c.Close()

	errCh <- nil
}

func dummyMsg(operation string, clientID string, data []byte) []byte {
	var msg consensus.RequestMsg
	msg.Timestamp = time.Now().UnixNano()
	msg.Operation = operation
	msg.ClientID = clientID
	msg.Data = string(data)

	// {"operation": "Op1", "clientID": "Client1", "data": "JJWEJPQOWJE", "timestamp": 190283901}
	jsonMsg, err := json.Marshal(&msg)
	if err != nil {
		log.Println(err)
		return nil
	}

	return []byte(jsonMsg)
}
