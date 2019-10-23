// TODO: secure connection such as HTTPS, or manual implementation
// from Section 5.2.2 Key Exchanges on TOCS.
package network
 import (
	"net/http"

	"bytes"
	"encoding/json"
	"fmt"

	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
)

type Server struct {
	url  string
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
		fmt.Printf("Node '%s' does not exist!\n", nodeID)
		return nil
	}

	node := NewNode(nodeID, nodeTable, viewID)
	server := &Server{nodeTable[nodeIdx].Url, node}

	server.setRoute()

	return server
}

func (server *Server) Start() {
	fmt.Printf("Server will be started at %s...\n", server.url)
	if err := http.ListenAndServe(server.url, nil); err != nil {
		fmt.Println(err)
		return
	}
}

func (server *Server) setRoute() {
	http.HandleFunc("/req", server.getReq)
	http.HandleFunc("/preprepare", server.getPrePrepare)
	http.HandleFunc("/prepare", server.getPrepare)
	http.HandleFunc("/commit", server.getCommit)
	http.HandleFunc("/reply", server.getReply)
	http.HandleFunc("/checkpoint", server.getCheckPoint)
	http.HandleFunc("/viewchange", server.getViewChange)
	http.HandleFunc("/newview", server.getNewView)

}

func (server *Server) getReq(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.RequestMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.MsgEntrance <- &msg
}

func (server *Server) getPrePrepare(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.PrePrepareMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.MsgEntrance <- &msg
}

func (server *Server) getPrepare(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.VoteMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}
	//fmt.Println(msg)

	server.node.MsgEntrance <- &msg
}

func (server *Server) getCommit(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.VoteMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.MsgEntrance <- &msg
}

func (server *Server) getReply(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.ReplyMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.GetReply(&msg)
}
func (server *Server) getCheckPoint(writer http.ResponseWriter, request *http.Request) {

	var msg consensus.CheckPointMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.MsgEntrance <- &msg
}

func (server *Server) getViewChange(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.ViewChangeMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}
	
	server.node.ViewMsgEntrance <- &msg
}

func (server *Server) getNewView(writer http.ResponseWriter, request *http.Request) {
	var msg consensus.NewViewMsg
	err := json.NewDecoder(request.Body).Decode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	server.node.ViewMsgEntrance <- &msg
}

func send(errCh chan<- error, c *http.Client, url string, msg []byte) {
	buff := bytes.NewBuffer(msg)

	resp, err := c.Post("http://"+url, "application/json", buff)
	errCh <- err

	if err == nil {
		defer resp.Body.Close()
	}
}

