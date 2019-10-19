package network

import (
	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
	"encoding/json"
	"fmt"
	"time"
	"errors"
	"sync"
)

type Node struct {
	NodeID        string
	NodeTable     []*NodeInfo
	View          *View
	States        map[int64]*consensus.State // key: sequenceID, value: state
	CommittedMsgs []*consensus.RequestMsg // kinda block.

	// Channels
	MsgEntrance   chan interface{}
	MsgDelivery   chan interface{}
	MsgExecution  chan *MsgPair
	MsgOutbound   chan *MsgOut
	MsgError      chan []error

	// Mutexes for preventing from concurrent access
	StatesMutex sync.Mutex
}

type NodeInfo struct {
	NodeID     string `json:"nodeID"`
	Url        string `json:"url"`
}

type View struct {
	ID             int64
	LastSequenceID int64
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

// Maximum timeout for any outbound messages.
const MaxOutboundTimeout = time.Millisecond * 500

func NewNode(nodeID string, nodeTable []*NodeInfo, viewID int64) *Node {
	node := &Node{
		NodeID: nodeID,
		NodeTable: nodeTable,
		View: &View{},

		// Consensus-related struct
		States: make(map[int64]*consensus.State),
		CommittedMsgs: make([]*consensus.RequestMsg, 0),

		// Channels
		MsgEntrance: make(chan interface{}, len(nodeTable) * 3),
		MsgDelivery: make(chan interface{}, len(nodeTable) * 3), // TODO: enough?
		MsgExecution: make(chan *MsgPair),
		MsgOutbound: make(chan *MsgOut),
		MsgError: make(chan []error),
	}

	node.updateView(viewID)

	for i := 0; i < 4; i++ {
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

	for _, nodeInfo := range node.NodeTable {
		if nodeInfo.NodeID == node.NodeID {
			continue
		}
		node.MsgOutbound <- &MsgOut{Path: nodeInfo.Url + path, Msg: jsonMsg}
	}
}

func (node *Node) Reply(msg *consensus.ReplyMsg) {
	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		node.MsgError <- []error{err}
		return
	}

	// Client가 없으므로, 일단 Primary에게 보내는 걸로 처리.
	node.MsgOutbound <- &MsgOut{Path: node.View.Primary.Url + "/reply", Msg: jsonMsg}
}

// Consensus start procedure for the Primary.
func (node *Node) GetReq(reqMsg *consensus.RequestMsg) error {
	LogMsg(reqMsg)

	// Create a new state for the new consensus.
	state, err := node.createStateForNewConsensus(reqMsg.Timestamp)
	if err != nil {
		return err
	}

	// Make the state prepared.
	prePrepareMsg, err := node.startConsensus(state, reqMsg)
	if err != nil {
		return err
	}

	// Register state into node and update last sequence number.
	node.StatesMutex.Lock()
	node.States[prePrepareMsg.SequenceID] = state
	node.View.LastSequenceID = reqMsg.SequenceID
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

	// Create a new state for the new consensus.
	state, err := node.createStateForNewConsensus(prePrepareMsg.RequestMsg.Timestamp)
	if err != nil {
		return err
	}

	// Make the state prepared.
	prePareMsg, err := node.prePrepare(state, prePrepareMsg)
	if err != nil {
		return err
	}

	// Register state into node and update last sequence number.
	node.StatesMutex.Lock()
	node.States[prePrepareMsg.SequenceID] = state
	if node.View.LastSequenceID < prePrepareMsg.SequenceID {
		node.View.LastSequenceID = prePrepareMsg.SequenceID
	}
	node.StatesMutex.Unlock()

	if prePareMsg != nil {
		// Attach node ID to the message
		prePareMsg.NodeID = node.NodeID

		LogStage("Pre-prepare", true)
		node.Broadcast(prePareMsg, "/prepare")
		LogStage("Prepare", false)
	}

	return nil
}

func (node *Node) GetPrepare(prepareMsg *consensus.VoteMsg) error {
	LogMsg(prepareMsg)

	node.StatesMutex.Lock()
	state := node.States[prepareMsg.SequenceID]
	node.StatesMutex.Unlock()

	if state == nil {
		return fmt.Errorf("[Prepare] State for sequence number %d has not created yet.", prepareMsg.SequenceID)
	}

	commitMsg, err := node.prepare(state, prepareMsg)
	if err != nil {
		return err
	}

	if commitMsg != nil {
		// Attach node ID to the message
		commitMsg.NodeID = node.NodeID

		LogStage("Prepare", true)
		node.Broadcast(commitMsg, "/commit")
		LogStage("Commit", false)
	}

	return nil
}

func (node *Node) GetCommit(commitMsg *consensus.VoteMsg) error {
	LogMsg(commitMsg)

	node.StatesMutex.Lock()
	state := node.States[commitMsg.SequenceID]
	node.StatesMutex.Unlock()

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
		replyMsg.NodeID = node.NodeID

		node.MsgExecution <- &MsgPair{replyMsg, committedMsg}
	}

	return nil
}

func (node *Node) GetReply(msg *consensus.ReplyMsg) {
	fmt.Printf("Result: %s by %s\n", msg.Result, msg.NodeID)
}

func (node *Node) createStateForNewConsensus(timeStamp int64) (*consensus.State, error) {
	// TODO: From TOCS: To guarantee exactly once semantics,
	// replicas discard requests whose timestamp is lower than
	// the timestamp in the last reply they sent to the client.

	LogStage("Create the replica status", true)

	return consensus.CreateState(node.View.ID, node.View.LastSequenceID, len(node.NodeTable), node.View.Primary.NodeID), nil
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
		if (node.NodeID == node.View.Primary.NodeID) {
			node.MsgDelivery <- msg
		}
	case *consensus.PrePrepareMsg:
		// Send pre-prepare message only if the node is not primary.
		if (node.NodeID != node.View.Primary.NodeID) {
			node.MsgDelivery <- msg
		}
	case *consensus.VoteMsg:
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
			// next sequence is not ready to execute.
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
			LogStage("Reply", true)

			// Delete the current message pair.
			delete(pairs, lastSequenceID)
		}

		// Print all committed messages.
		for idx, v := range committedMsgs {
			fmt.Printf("committedMsgs[%d]: %#v\n", idx, v)
		}
	}
}

func (node *Node) sendMsg() {
	for {
		msg := <-node.MsgOutbound
		errCh := make(chan error, 1)

		go func() {
			send(errCh, msg.Path, msg.Msg)
		}()

		select {
		case err := <-errCh:
			if err != nil {
				node.MsgError <- []error{err}
				// TODO: view change.
			}
		case <-time.After(MaxOutboundTimeout):
			node.MsgError <- []error{errors.New("out of time :(")}
			// TODO: view change.
		}
	}
}

func (node *Node) logErrorMsg() {
	for {
		errs := <-node.MsgError
		for _, err := range errs {
			fmt.Println(err)
		}
	}
}

func (node *Node) startConsensus(state consensus.PBFT, reqMsg *consensus.RequestMsg) (*consensus.PrePrepareMsg, error) {
	return state.StartConsensus(reqMsg)
}

func (node *Node) prePrepare(state consensus.PBFT, prePrepareMsg *consensus.PrePrepareMsg) (*consensus.VoteMsg, error) {
	// TODO: From TOCS: sequence number n is between a low water mark h
	// and a high water mark H. The last condition is necessary to enable
	// garbage collection and to prevent a faulty primary from exhausting
	// the space of sequence numbers by selecting a very large one.

	return state.PrePrepare(prePrepareMsg)
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
	node.View.LastSequenceID = 0
	viewIdx := viewID % int64(len(node.NodeTable))
	node.View.Primary = node.NodeTable[viewIdx]

	fmt.Println("ViewID:", node.View.ID, "Primary:", node.View.Primary.NodeID)
}
