package network

import (
	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
	"encoding/json"
	"net"
	"net/http"
	"fmt"
	"time"
	"errors"
	"sync"
	"sync/atomic"
)

type Node struct {
	MyInfo          *NodeInfo
	NodeTable       []*NodeInfo
	View            *View
	States          map[int64]*consensus.State // key: sequenceID, value: state
	ViewChangeState *consensus.ViewChangeState
	CommittedMsgs   []*consensus.RequestMsg // kinda block.
	TotalConsensus  int64 // atomic. number of consensus started so far.
	HttpClient      *http.Client

	// Channels
	MsgEntrance   chan interface{}
	MsgDelivery   chan interface{}
	MsgExecution  chan *MsgPair
	MsgOutbound   chan *MsgOut
	MsgError      chan []error

	// Mutexes for preventing from concurrent access
	StatesMutex sync.RWMutex

	// CheckpointMsg save
	StableCheckPoint    int64
	CheckPointSendPoint int64
	CheckPointMsgsLog   map[int64]map[string]*consensus.CheckPointMsg
}

type NodeInfo struct {
	NodeID     string `json:"nodeID"`
	Url        string `json:"url"`
}

type View struct {
	ID             int64
	Primary        *NodeInfo
}

type MsgPair struct {
	replyMsg     *consensus.ReplyMsg
	committedMsg *consensus.RequestMsg
}

// Outbound message
type MsgOut struct {
	Path string
	Msg  []byte
}

const periodCheckPoint = 5

// Cooling time to escape frequent error, or message sending retry.
const CoolingTime = time.Millisecond * 20

// Number of error messages to start cooling.
const CoolingTotalErrMsg = 100

// Number of outbound connection for a node.
const MaxOutboundConnection = 1000

func NewNode(myInfo *NodeInfo, nodeTable []*NodeInfo, viewID int64) *Node {
	node := &Node{
		MyInfo: myInfo,
		NodeTable: nodeTable,
		View: &View{},

		// Consensus-related struct
		States: make(map[int64]*consensus.State),
		CommittedMsgs: make([]*consensus.RequestMsg, 0),
		ViewChangeState: nil,

		// Channels
		MsgEntrance: make(chan interface{}, len(nodeTable) * 3),
		MsgDelivery: make(chan interface{}, len(nodeTable) * 3), // TODO: enough?
		MsgExecution: make(chan *MsgPair),
		MsgOutbound: make(chan *MsgOut),
		MsgError: make(chan []error),
		StableCheckPoint:  0,
		CheckPointMsgsLog: make(map[int64]map[string]*consensus.CheckPointMsg),
	}

	node.HttpClient = &http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 10 * time.Second,
			}).Dial,
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	atomic.StoreInt64(&node.TotalConsensus, 0)
	node.updateView(viewID)

	for i := 0; i < 5; i++ {
		// Start message dispatcher
		go node.dispatchMsg()

		// Start message resolver
		go node.resolveMsg()
	}

	// Start message executor
	go node.executeMsg()

	// Start outbound message sender
	go node.sendMsg()

	// Start message error logger
	go node.logErrorMsg()

	// TODO:
	// From TOCS: The backups check the sequence numbers assigned by
	// the primary and use timeouts to detect when it stops.
	// They trigger view changes to select a new primary when it
	// appears that the current one has failed.

	return node
}

// Broadcast marshalled message.
func (node *Node) Broadcast(msg interface{}, path string) {
	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		node.MsgError <- []error{err}
		return
	}

	node.MsgOutbound <- &MsgOut{Path: node.MyInfo.Url + path, Msg: jsonMsg}
}

func (node *Node) Reply(msg *consensus.ReplyMsg) {
	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		node.MsgError <- []error{err}
		return
	}

	// Broadcast reply.
	node.MsgOutbound <- &MsgOut{Path: node.MyInfo.Url + "/reply", Msg: jsonMsg}

	//ViewChange for test
	node.StartViewChange()
}

// Consensus start procedure for the Primary.
func (node *Node) GetReq(reqMsg *consensus.RequestMsg) error {
	LogMsg(reqMsg)

	// Create a new state object.
	state, err := node.createState(reqMsg.Timestamp)
	if err != nil {
		return err
	}

	// Fill sequence number into the state and make the state prepared.
	prePrepareMsg, err := node.startConsensus(state, reqMsg)
	if err != nil {
		return err
	}

	// Register state into node and update last sequence number.
	node.StatesMutex.Lock()
	node.States[prePrepareMsg.SequenceID] = state
	node.StatesMutex.Unlock()

	LogStage(fmt.Sprintf("Consensus Process (ViewID: %d, Primary: %s)",
	         node.View.ID, node.View.Primary.NodeID), false)

	// Send getPrePrepare message
	if prePrepareMsg != nil {
		node.Broadcast(prePrepareMsg, "/preprepare")
		LogStage("Pre-prepare", true)
	}

	return nil
}

// Consensus start procedure for normal participants.
func (node *Node) GetPrePrepare(prePrepareMsg *consensus.PrePrepareMsg) error {
	LogMsg(prePrepareMsg)

	// Create a new state object.
	state, err := node.createState(prePrepareMsg.RequestMsg.Timestamp)
	if err != nil {
		return err
	}

	// Fill sequence number into the state and make the state prepared.
	prepareMsg, err := node.prePrepare(state, prePrepareMsg)
	if err != nil {
		return err
	}

	// Register state into node and update last sequence number.
	node.StatesMutex.Lock()
	node.States[prePrepareMsg.SequenceID] = state
	node.StatesMutex.Unlock()

	if prepareMsg != nil {
		// Attach node ID to the message
		prepareMsg.NodeID = node.MyInfo.NodeID

		LogStage("Pre-prepare", true)
		node.Broadcast(prepareMsg, "/prepare")
		LogStage("Prepare", false)
	}

	return nil
}

func (node *Node) GetPrepare(prepareMsg *consensus.VoteMsg) error {
	LogMsg(prepareMsg)

	node.StatesMutex.RLock()
	state := node.States[prepareMsg.SequenceID]
	node.StatesMutex.RUnlock()

	if state == nil {
		return fmt.Errorf("[Prepare] State for sequence number %d has not created yet.", prepareMsg.SequenceID)
	}

	commitMsg, err := node.prepare(state, prepareMsg)
	if err != nil {
		return err
	}

	if commitMsg != nil {
		// Attach node ID to the message
		commitMsg.NodeID = node.MyInfo.NodeID

		LogStage("Prepare", true)
		node.Broadcast(commitMsg, "/commit")
		LogStage("Commit", false)
	}

	return nil
}

func (node *Node) GetCommit(commitMsg *consensus.VoteMsg) error {
	LogMsg(commitMsg)

	node.StatesMutex.RLock()
	state := node.States[commitMsg.SequenceID]
	node.StatesMutex.RUnlock()

	if state == nil {
		return fmt.Errorf("[Commit] State for sequence number %d has not created yet.", commitMsg.SequenceID)
	}

	replyMsg, committedMsg, err := node.commit(state, commitMsg)
	if err != nil {
		return err
	}

	if replyMsg != nil {
		if committedMsg == nil {
			return errors.New("committed message is nil, even though the reply message is not nil")
		}

		// Attach node ID to the message
		replyMsg.NodeID = node.MyInfo.NodeID

		node.MsgExecution <- &MsgPair{replyMsg, committedMsg}
	}

	return nil
}

func (node *Node) GetReply(msg *consensus.ReplyMsg) {
	fmt.Printf("Result: %s by %s\n", msg.Result, msg.NodeID)
}

func (node *Node) StartViewChange() {

	//Start_ViewChange
	LogStage("ViewChange", false) //ViewChange_Start

	//Change View and Primary
	node.updateView(node.View.ID + 1)

	//Create ViewChangeState
	node.ViewChangeState = consensus.CreateViewChangeState(node.MyInfo.NodeID, len(node.NodeTable), node.View.ID)

	//Create ViewChangeMsg
	viewChangeMsg, err := node.ViewChangeState.CreateViewChangeMsg()
	if err != nil {
		node.MsgError <- []error{err}
		return
	}

	node.Broadcast(viewChangeMsg, "/viewchange")
}

func (node *Node) NewView(newviewMsg *consensus.NewViewMsg) error {
	LogMsg(newviewMsg)

	node.Broadcast(newviewMsg, "/newview")
	LogStage("NewView", true)

	return nil
}

func (node *Node) GetViewChange(viewchangeMsg *consensus.ViewChangeMsg) error {
	LogMsg(viewchangeMsg)

	if node.ViewChangeState == nil || node.ViewChangeState.CurrentStage != consensus.ViewChanged {
		return nil
	}

	//newViewMsg, err := node.ViewChangeState.ViewChange(viewchangeMsg)
	newView, err := node.ViewChangeState.ViewChange(viewchangeMsg)
	if err != nil {
		return err
	}

	LogStage("ViewChange", true)

	if newView != nil && node.View.Primary.NodeID == node.MyInfo.NodeID {

		LogStage("NewView", false)
		node.NewView(newView)

	}

	return nil
}

func (node *Node) GetNewView(msg *consensus.NewViewMsg) error {
	fmt.Printf("NewView: %d by %s\n", msg.NextViewID, msg.NodeID)
	return nil
}

func (node *Node) createState(timeStamp int64) (*consensus.State, error) {
	// TODO: From TOCS: To guarantee exactly once semantics,
	// replicas discard requests whose timestamp is lower than
	// the timestamp in the last reply they sent to the client.

	LogStage("Create the replica status", true)

	return consensus.CreateState(node.View.ID, node.MyInfo.NodeID, len(node.NodeTable)), nil
}

func (node *Node) dispatchMsg() {
	for {
		select {
		case msg := <-node.MsgEntrance:
			node.routeMsg(msg)
		}
	}
}

func (node *Node) routeMsg(msgEntered interface{}) {
	switch msg := msgEntered.(type) {
	case *consensus.RequestMsg:
		// Send request message only if the node is primary.
		if node.MyInfo.NodeID == node.View.Primary.NodeID {
			node.MsgDelivery <- msg
		}
	case *consensus.PrePrepareMsg:
		// Send pre-prepare message only if the node is not primary.
		if node.MyInfo.NodeID != node.View.Primary.NodeID {
			node.MsgDelivery <- msg
		}
	case *consensus.VoteMsg:
		// Messages are broadcasted from the node, so
		// the message sent to itself can exist.
		if node.MyInfo.NodeID != msg.NodeID {
			node.MsgDelivery <- msg
		}
	case *consensus.ReplyMsg:
		node.MsgDelivery <- msg
	case *consensus.CheckPointMsg:
		node.States[int64(msg.SequenceID)].MsgLogs.CheckPointMutex.Lock()
		node.CheckPoint(msg.SequenceID, msg, 2)
		node.States[int64(msg.SequenceID)].MsgLogs.CheckPointMutex.Unlock()
	case *consensus.ViewChangeMsg:
		node.MsgDelivery <- msg
	case *consensus.NewViewMsg:
		node.MsgDelivery <- msg
	}
}

func (node *Node) resolveMsg() {
	for {
		var err error
		msgDelivered := <-node.MsgDelivery

		// Resolve the message.
		switch msg := msgDelivered.(type) {
		case *consensus.RequestMsg:
			err = node.GetReq(msg)
		case *consensus.PrePrepareMsg:
			err = node.GetPrePrepare(msg)
		case *consensus.VoteMsg:
			if msg.MsgType == consensus.PrepareMsg {
				err = node.GetPrepare(msg)
			} else if msg.MsgType == consensus.CommitMsg {
				err = node.GetCommit(msg)
			}
		case *consensus.ReplyMsg:
			node.GetReply(msg)
		case *consensus.ViewChangeMsg:
			err = node.GetViewChange(msg)
		case *consensus.NewViewMsg:
			err = node.GetNewView(msg)
		}

		if err != nil {
			// Print error.
			node.MsgError <- []error{err}
			// Send message into dispatcher.
			node.MsgEntrance <- msgDelivered
		}
	}
}

// Fill the result field, after all execution for
// other states which the sequence number is smaller,
// i.e., the sequence number of the last committed message is
// one smaller than the current message.
func (node *Node) executeMsg() {
	var committedMsgs []*consensus.RequestMsg
	pairs := make(map[int64]*MsgPair)

	for {
		msgPair := <-node.MsgExecution
		pairs[msgPair.committedMsg.SequenceID] = msgPair
		committedMsgs = make([]*consensus.RequestMsg, 0)

		// Execute operation for all the consecutive messages.
		for {
			var lastSequenceID int64

			// Find the last committed message.
			msgTotalCnt := len(node.CommittedMsgs)
			if msgTotalCnt > 0 {
				lastCommittedMsg := node.CommittedMsgs[msgTotalCnt - 1]
				lastSequenceID = lastCommittedMsg.SequenceID
			} else {
				lastSequenceID = 0
			}

			// Stop execution if the message for the
			// current sequence is not ready to execute.
			p := pairs[lastSequenceID + 1]
			if p == nil {
				break
			}

			// Add the committed message in a private log queue
			// to print the orderly executed messages.
			committedMsgs = append(committedMsgs, p.committedMsg)
			LogStage("Commit", true)

			// TODO: execute appropriate operation.
			p.replyMsg.Result = "Executed"

			// After executing the operation, log the
			// corresponding committed message to node.
			node.CommittedMsgs = append(node.CommittedMsgs, p.committedMsg)

			node.Reply(p.replyMsg)

			if node.CommittedMsgs[len(node.CommittedMsgs)-1].SequenceID == node.CheckPointSendPoint+periodCheckPoint {
				node.CheckPointSendPoint = node.CheckPointSendPoint + periodCheckPoint

				SequenceID := node.CommittedMsgs[len(node.CommittedMsgs)-1].SequenceID

				fmt.Println("Start Check Point! ")
				node.States[node.CommittedMsgs[len(node.CommittedMsgs)-1].SequenceID].MsgLogs.CheckPointMutex.Lock()
				checkPointMsg, _ := node.getCheckPointMsg(SequenceID, node.MyInfo.NodeID, node.CommittedMsgs[len(node.CommittedMsgs)-1])

				node.Broadcast(checkPointMsg, "/checkpoint")
				node.CheckPoint(SequenceID, checkPointMsg, 1)
				node.States[node.CommittedMsgs[len(node.CommittedMsgs)-1].SequenceID].MsgLogs.CheckPointMutex.Unlock()

			}
			LogStage("Reply", true)

			// Delete the current message pair.
			delete(pairs, lastSequenceID + 1)
		}

		// Print all committed messages.
		for _, v := range committedMsgs {
			digest, _ := consensus.Digest(v.Data)
			fmt.Printf("***committedMsgs[%d]: clientID=%s, operation=%s, timestamp=%d, data(digest)=%s***\n",
			           v.SequenceID, v.ClientID, v.Operation, v.Timestamp, digest)
		}
	}
}

func (node *Node) sendMsg() {
	sem := make(chan bool, MaxOutboundConnection)

	for {
		msg := <-node.MsgOutbound

		// Goroutine for concurrent broadcast() with timeout
		sem <- true
		go func() {
			defer func() { <-sem }()
			errCh := make(chan error, 1)

			// Goroutine for concurrent broadcast()
			go func() {
				broadcast(errCh, msg.Path, msg.Msg)
			}()

			select {
			case err := <-errCh:
				if err != nil {
					node.MsgError <- []error{err}
					// TODO: view change.
				}
			}
		}()
	}
}

func (node *Node) logErrorMsg() {
	coolingMsgLeft := CoolingTotalErrMsg

	for {
		errs := <-node.MsgError
		for _, err := range errs {
			coolingMsgLeft--
			if coolingMsgLeft == 0 {
				fmt.Printf("%d error messages detected! cool down for %d milliseconds\n",
				           CoolingTotalErrMsg, CoolingTime / time.Millisecond)
				time.Sleep(CoolingTime)
				coolingMsgLeft = CoolingTotalErrMsg
			}
			fmt.Println(err)
		}
	}
}

func (node *Node) startConsensus(state consensus.PBFT, reqMsg *consensus.RequestMsg) (*consensus.PrePrepareMsg, error) {
	// Increment the number of consensus atomically in the current view.
	// TODO: Currently, StartConsensus must succeed.
	newTotalConsensus := atomic.AddInt64(&node.TotalConsensus, 1)

	return state.StartConsensus(reqMsg, newTotalConsensus)
}

func (node *Node) prePrepare(state consensus.PBFT, prePrepareMsg *consensus.PrePrepareMsg) (*consensus.VoteMsg, error) {
	// TODO: From TOCS: sequence number n is between a low water mark h
	// and a high water mark H. The last condition is necessary to enable
	// garbage collection and to prevent a faulty primary from exhausting
	// the space of sequence numbers by selecting a very large one.

	prepareMsg, err := state.PrePrepare(prePrepareMsg)
	if err != nil {
		return nil, err
	}

	// Increment the number of consensus atomically in the current view.
	atomic.AddInt64(&node.TotalConsensus, 1)

	return prepareMsg, err
}

// Even though the state has passed prepare stage, the node can receive
// PREPARE messages from backup servers which consensus are slow.
func (node *Node) prepare(state consensus.PBFT, prepareMsg *consensus.VoteMsg) (*consensus.VoteMsg, error) {
	return state.Prepare(prepareMsg)
}

// Even though the state has passed commit stage, the node can receive
// COMMIT messages from backup servers which consensus are slow.
func (node *Node) commit(state consensus.PBFT, commitMsg *consensus.VoteMsg) (*consensus.ReplyMsg, *consensus.RequestMsg, error) {
	return state.Commit(commitMsg)
}

func (node *Node) updateView(viewID int64) {
	node.View.ID = viewID
	viewIdx := viewID % int64(len(node.NodeTable))
	node.View.Primary = node.NodeTable[viewIdx]

	fmt.Println("ViewID:", node.View.ID, "Primary:", node.View.Primary.NodeID)
}
func (node *Node) getCheckPointMsg(SequenceID int64, nodeID string, ReqMsgs *consensus.RequestMsg) (*consensus.CheckPointMsg, error) {

	digest, err := consensus.Digest(ReqMsgs)
	if err != nil {
		return nil, err
	}

	return &consensus.CheckPointMsg{
		SequenceID: SequenceID,
		Digest:     digest,
		NodeID:     nodeID,
	}, nil
}
func (node *Node) Checkpointchk(SequenceID int64) bool {

	if len(node.CheckPointMsgsLog[int64(SequenceID)]) >= (2*node.States[SequenceID].F+1) && node.CheckPointMsgsLog[int64(SequenceID)][node.MyInfo.NodeID] != nil {

		return true
	}

	return false
}
func (node *Node) CheckPoint(SequenceID int64, msg *consensus.CheckPointMsg, typea int) {

	if node.CheckPointMsgsLog[int64(SequenceID)] == nil {
		node.CheckPointMsgsLog[int64(SequenceID)] = make(map[string]*consensus.CheckPointMsg)
	}
	node.CheckPointMsgsLog[int64(SequenceID)][msg.NodeID] = msg
	// fmt.Println("Save Checkpoint Msg : ", node.CheckPointMsgsLog[int64(SequenceID)][node.MyInfo.NodeID], "// length : ", len(node.CheckPointMsgsLog[int64(SequenceID)]), " type :", typea)
	if node.Checkpointchk(SequenceID) && node.States[SequenceID].CheckPointState == 0 {

		node.States[SequenceID].CheckPointState = 1
		for v, _ := range node.CheckPointMsgsLog {
			// fmt.Println(" Sequence N : ", v, "Stable Checkpoint :", node.StableCheckPoint)
			if int64(v) < (node.StableCheckPoint + periodCheckPoint) {
				delete(node.CheckPointMsgsLog, v)
			}
		}
		for v, _ := range node.States {
			if int64(v) < (node.StableCheckPoint + periodCheckPoint) {
				delete(node.States, v)
			}
		}
		// fmt.Println("node.States[int64(msg.SequenceID)] 222: ", node.States[int64(msg.SequenceID)], " Sequence ID :", msg.SequenceID)
		node.StableCheckPoint = SequenceID
		fmt.Println("CheckPoint Success!! => ", typea, "length", len(node.CheckPointMsgsLog[int64(SequenceID)]))

	}
	if len(node.CheckPointMsgsLog[int64(SequenceID)]) == 4 {
		fmt.Println("CheckPoint History!! ", typea)
		// fmt.Println("len(node.CheckPointMsgsLog[int(msg.SequenceID)]) : ", len(node.CheckPointMsgsLog[int64(SequenceID)]), " Sequence ID :", SequenceID)
		for v, _ := range node.CheckPointMsgsLog {
			fmt.Println(" Sequence N : ", v)

			for _, j := range node.CheckPointMsgsLog[v] {
				fmt.Println("    === >", j)
			}

		}
		fmt.Println("MsgLogs History!!")
		for v, _ := range node.States {
			fmt.Println(" Sequence N : ", v)
			fmt.Println("    === > ReqMsgs : ", node.States[v].MsgLogs.ReqMsg)
			fmt.Println("    === > Preprepare : ", node.States[v].MsgLogs.PrePrepareMsg)
			fmt.Println("    === > Prepare : ", node.States[v].MsgLogs.PrepareMsgs)
			fmt.Println("    === > Commit : ", node.States[v].MsgLogs.CommitMsgs)
		}
	}
}
