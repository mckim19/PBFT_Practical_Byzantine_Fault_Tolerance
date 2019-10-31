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
	"crypto/ecdsa"
)

type Server struct {
	url  string
	node *Node
}

func NewServer(nodeID string, nodeTable []*NodeInfo, viewID int64, decodePrivKey *ecdsa.PrivateKey) *Server {
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

	node := NewNode(nodeTable[nodeIdx], nodeTable, viewID, decodePrivKey)
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

		var marshalledMsg []byte
		var ok bool
		switch path {
		case "/req":
			var msg consensus.RequestMsg
			marshalledMsg, err, ok = deattachSignatureMsg(message, nodeInfo.PubKey)
			if err != nil || ok == false {
				break
			}
			_ = json.Unmarshal(marshalledMsg, &msg)
			server.node.MsgEntrance <- &msg
		case "/preprepare":
			var msg consensus.PrePrepareMsg
			marshalledMsg, err, ok = deattachSignatureMsg(message, nodeInfo.PubKey)
			if err != nil || ok == false {
				break
			}
			_ = json.Unmarshal(marshalledMsg, &msg)
			server.node.MsgEntrance <- &msg
		case "/prepare":
			var msg consensus.VoteMsg
			marshalledMsg, err, ok = deattachSignatureMsg(message, nodeInfo.PubKey)
			if err != nil || ok == false {
				break
			}
			_ = json.Unmarshal(marshalledMsg, &msg)
			server.node.MsgEntrance <- &msg
		case "/commit":
			var msg consensus.VoteMsg
			marshalledMsg, err, ok = deattachSignatureMsg(message, nodeInfo.PubKey)
			if err != nil || ok == false {
				break
			}
			_ = json.Unmarshal(marshalledMsg, &msg)
			server.node.MsgEntrance <- &msg
		case "/reply":
			var msg consensus.ReplyMsg
			marshalledMsg, err, ok = deattachSignatureMsg(message, nodeInfo.PubKey)
			if err != nil || ok == false {
				break
			}
			_ = json.Unmarshal(marshalledMsg, &msg)
			server.node.MsgEntrance <- &msg
		case "/checkpoint":
			var msg consensus.CheckPointMsg
			marshalledMsg, err, ok = deattachSignatureMsg(message, nodeInfo.PubKey)
			if err != nil || ok == false {
				break
			}
			_ = json.Unmarshal(marshalledMsg, &msg)
			server.node.MsgEntrance <- &msg
		case "/viewchange":
			var msg consensus.ViewChangeMsg
			marshalledMsg, err, ok = deattachSignatureMsg(message, nodeInfo.PubKey)
			if err != nil || ok == false {
				break
			}
			_ = json.Unmarshal(marshalledMsg, &msg)
			server.node.ViewMsgEntrance <- &msg
		case "/newview":
			var msg consensus.NewViewMsg
			marshalledMsg, err, ok = deattachSignatureMsg(message, nodeInfo.PubKey)
			if err != nil || ok == false {
				break
			}
			_ = json.Unmarshal(marshalledMsg, &msg)
			server.node.ViewMsgEntrance <- &msg
		}

		if err != nil {
			log.Println(err)
		}
	}
}

func (server *Server) sendDummyMsg() {
	// Set periodic send signal.
	ticker := time.NewTicker(time.Millisecond * 500)
	defer ticker.Stop()

	// Create a dummy data.
	data := make([]byte, 1 << 20)
	for i := range data {
		data[i] = 'A'
	}
	data[len(data) - 1] = 0

	currentView := server.node.View.ID

	// Current node sends dummy message when private view (currentView)
	// is primary. (e.g., if the node index in the node table is 3,
	// and current view ID is 1, the third dummy request message
	// is sent from the current node.)
	for {
		select {
		case <-ticker.C:
			// Send message from the current (changed) primary node.
			// Changing view must precedes sending request message.
			primaryNode := server.node.getPrimaryByID(currentView)

			// TODO: change view based on that of the current node.
			currentView++
			if primaryNode.NodeID != server.node.MyInfo.NodeID {
				continue
			}

			// Create a dummy message.
			u := primaryNode.Url + "/req"
			dummy := dummyMsg("Op1", primaryNode.NodeID, data)

			// Broadcast the dummy message.
			errCh := make(chan error, 1)
			log.Printf("Broadcasting dummy message from %s", u)
			broadcast(errCh, u, dummy, server.node.PrivKey)
			err := <-errCh
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func broadcast(errCh chan<- error, url string, msg []byte, privKey *ecdsa.PrivateKey) {
	sigMgsBytes := attachSignatureMsg(msg, privKey)
	url = "ws://" + url // Fix using url.URL{}
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		errCh <- err
		return
	}

	err = c.WriteMessage(websocket.TextMessage, sigMgsBytes)
	if err != nil {
		errCh <- err
		return
	}
	defer c.Close()

	errCh <- nil
}

func attachSignatureMsg(msg []byte, privKey *ecdsa.PrivateKey) []byte{
	var sigMgs consensus.SignatureMsg
	// msg signature
	r, s, signature, err := consensus.Signature(privKey, string(msg))
	if err == nil {
		// setting SignatureMsg
		sigMgs = consensus.SignatureMsg {
			Signature: signature,
			R: r,
			S: s,
			MarshalledMsg: msg,
		}
	}
	sigMgsBytes, _ := json.Marshal(sigMgs)

	return sigMgsBytes
}

func deattachSignatureMsg(msg []byte, pubkey *ecdsa.PublicKey) ([]byte, error, bool) {
	var sigMgs consensus.SignatureMsg
	// unmarshal sigmsgs
	err := json.Unmarshal(msg, &sigMgs)
	if err != nil {
		return nil, err, false
	}
	// msg VerifySignature
	ok := consensus.VerifySignature(*pubkey, sigMgs.R, sigMgs.S, string(sigMgs.MarshalledMsg))
	return sigMgs.MarshalledMsg, nil, ok
}
func dummyMsg(operation string, clientID string, data []byte) []byte {
	var msg consensus.RequestMsg
	msg.Operation = operation
	msg.ClientID = clientID
	msg.Data = string(data)
	msg.Timestamp = time.Now().UnixNano()

	// {"operation": "Op1", "clientID": "Client1", "data": "JJWEJPQOWJE", "timestamp": 190283901}
	jsonMsg, err := json.Marshal(&msg)
	if err != nil {
		log.Println(err)
		return nil
	}

	return []byte(jsonMsg)
}
