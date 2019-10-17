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
	MsgBuffer     *MsgBuffer

	// Channels
	MsgEntrance   chan interface{}
	MsgDelivery   chan interface{}
	MsgExecution  chan *MsgPair
	MsgOutbound   chan *MsgOut
	MsgError      chan []error
	Alarm         chan bool
}

type NodeInfo struct {
	NodeID     string
	Url        string
}

type MsgBuffer struct {
	ReqMsgsMutex        sync.Mutex
	PrePrepareMsgsMutex sync.Mutex
	PrepareMsgsMutex    sync.Mutex
	CommitMsgsMutex     sync.Mutex

	ReqMsgs        []*consensus.RequestMsg
	PrePrepareMsgs []*consensus.PrePrepareMsg
	PrepareMsgs    []*consensus.VoteMsg
	CommitMsgs     []*consensus.VoteMsg
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

// Maximum delay between dispatching and delivering messages.
const ResolvingTimeDuration = time.Millisecond * 1000

// Maximum timeout for any outbound messages.
const MaxTimeout = time.Millisecond * 500

// Maximum batch size of messages for creating new consensus.
const BatchMax = 2

func NewNode(nodeID string, nodeTable []*NodeInfo, viewID int64) *Node {
	node := &Node{
		NodeID: nodeID,
		NodeTable: nodeTable,
		View: &View{},

		// Consensus-related struct
		States: make(map[int64]*consensus.State),
		CommittedMsgs: make([]*consensus.RequestMsg, 0),
		MsgBuffer: &MsgBuffer{
			ReqMsgsMutex:        sync.Mutex{},
			PrePrepareMsgsMutex: sync.Mutex{},
			PrepareMsgsMutex:    sync.Mutex{},
			CommitMsgsMutex:     sync.Mutex{},

			ReqMsgs:        make([]*consensus.RequestMsg, 0),
			PrePrepareMsgs: make([]*consensus.PrePrepareMsg, 0),
			PrepareMsgs:    make([]*consensus.VoteMsg, 0),
			CommitMsgs:     make([]*consensus.VoteMsg, 0),
		},

		// Channels
		MsgEntrance: make(chan interface{}),
		MsgDelivery: make(chan interface{}, len(nodeTable) * 3), // TODO: enough?
		MsgExecution: make(chan *MsgPair),
		MsgOutbound: make(chan *MsgOut),
		MsgError: make(chan []error),
		Alarm: make(chan bool, 1),
	}

	node.updateView(viewID)

	// Start message dispatcher
	go node.dispatchMsg()

	// Start alarm trigger
	go node.alarmToDispatcher()

	// Start message resolver
	go node.resolveMsg()

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
	node.States[prePrepareMsg.SequenceID] = state
	node.View.LastSequenceID = reqMsg.SequenceID

	LogStage(fmt.Sprintf("Consensus Process (ViewID: %d, Primary: %s)",
		 state.ViewID, node.View.Primary.NodeID), false)

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
	node.States[prePrepareMsg.SequenceID] = state
	if node.View.LastSequenceID < prePrepareMsg.SequenceID {
		node.View.LastSequenceID = prePrepareMsg.SequenceID
	}

	if prePareMsg != nil {
		// Attach node ID to the message
		prePareMsg.NodeID = node.NodeID

		LogStage("Pre-prepare", true)
		node.Broadcast(prePareMsg, "/prepare")
		LogStage("Prepare", false)

		// !!!HACK!!!: Add PREPARE pseudo-message from Primary node
		// because Primary node does not send the PREPARE message.
		state.MsgLogs.PrepareMsgs[node.View.Primary.NodeID] = nil
	}

	return nil
}

func (node *Node) GetPrepare(prepareMsg *consensus.VoteMsg) error {
	LogMsg(prepareMsg)

	state := node.States[prepareMsg.SequenceID]
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

	state := node.States[commitMsg.SequenceID]
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

	return consensus.CreateState(node.View.ID, node.View.LastSequenceID), nil
}

func (node *Node) dispatchMsg() {
	for {
		select {
		case msg := <-node.MsgEntrance:
			node.routeMsg(msg)
		case <-node.Alarm:
			node.routeMsgWhenAlarmed()
		}
	}
}

func (node *Node) routeMsg(msgEntered interface{}) {
	switch msg := msgEntered.(type) {
	case *consensus.RequestMsg:
		// Skip if the node is not primary
		if (node.NodeID != node.View.Primary.NodeID) {
			break
		}

		node.MsgBuffer.ReqMsgsMutex.Lock()
		node.MsgBuffer.ReqMsgs = append(node.MsgBuffer.ReqMsgs, msg)
		node.MsgBuffer.ReqMsgsMutex.Unlock()
	case *consensus.PrePrepareMsg:
		// Skip if the node is primary
		if (node.NodeID == node.View.Primary.NodeID) {
			break
		}

		node.MsgBuffer.PrePrepareMsgsMutex.Lock()
		node.MsgBuffer.PrePrepareMsgs = append(node.MsgBuffer.PrePrepareMsgs, msg)
		node.MsgBuffer.PrePrepareMsgsMutex.Unlock()
	case *consensus.VoteMsg:
		if msg.MsgType == consensus.PrepareMsg {
			node.MsgBuffer.PrepareMsgsMutex.Lock()
			node.MsgBuffer.PrepareMsgs = append(node.MsgBuffer.PrepareMsgs, msg)
			node.MsgBuffer.PrepareMsgsMutex.Unlock()
		} else if msg.MsgType == consensus.CommitMsg {
			node.MsgBuffer.CommitMsgsMutex.Lock()
			node.MsgBuffer.CommitMsgs = append(node.MsgBuffer.CommitMsgs, msg)
			node.MsgBuffer.CommitMsgsMutex.Unlock()
		}
	}
}

// Buffered messages for each consensus stage are sequentially processed,
// starting from getting new request to replying them.
func (node *Node) routeMsgWhenAlarmed() {
	// Check ReqMsgs, send them.
	if len(node.MsgBuffer.ReqMsgs) != 0 {
		node.deliveryRequestMsgs()
	}

	// Check PrePrepareMsgs, send them.
	if len(node.MsgBuffer.PrePrepareMsgs) != 0 {
		node.deliveryPrePrepareMsgs()
	}

	// Check PrepareMsgs, send them.
	if len(node.MsgBuffer.PrepareMsgs) != 0 {
		node.deliveryPrepareMsgs()
	}

	// Check CommitMsgs, send them.
	if len(node.MsgBuffer.CommitMsgs) != 0 {
		node.deliveryCommitMsgs()
	}
}

func (node *Node) deliveryRequestMsgs() {
	// Copy buffered messages with buffer locked.
	node.MsgBuffer.ReqMsgsMutex.Lock()
	msgTotalCnt := len(node.MsgBuffer.ReqMsgs)
	msgBatchCnt := BatchMax
	if msgBatchCnt > msgTotalCnt {
		msgBatchCnt = msgTotalCnt
	}
	msgs, buffer := node.MsgBuffer.ReqMsgs[:msgBatchCnt],
	                node.MsgBuffer.ReqMsgs[msgBatchCnt:]

	// Pop msgs from the buffer and release the buffer lock.
	node.MsgBuffer.ReqMsgs = buffer
	node.MsgBuffer.ReqMsgsMutex.Unlock()

	// Send messages.
	node.MsgDelivery <- msgs
}

func (node *Node) deliveryPrePrepareMsgs() {
	// Copy buffered messages with buffer locked.
	node.MsgBuffer.PrePrepareMsgsMutex.Lock()
	msgTotalCnt := len(node.MsgBuffer.PrePrepareMsgs)
	msgBatchCnt := BatchMax
	if msgBatchCnt > msgTotalCnt {
		msgBatchCnt = msgTotalCnt
	}
	msgs, buffer := node.MsgBuffer.PrePrepareMsgs[:msgBatchCnt],
	                node.MsgBuffer.PrePrepareMsgs[msgBatchCnt:]

	// Pop msgs from the buffer and release the buffer lock.
	node.MsgBuffer.PrePrepareMsgs = buffer
	node.MsgBuffer.PrePrepareMsgsMutex.Unlock()

	// Send messages.
	node.MsgDelivery <- msgs
}

func (node *Node) deliveryPrepareMsgs() {
	// Copy buffered messages with buffer locked.
	node.MsgBuffer.PrepareMsgsMutex.Lock()
	msgs := make([]*consensus.VoteMsg, len(node.MsgBuffer.PrepareMsgs))
	copy(msgs, node.MsgBuffer.PrepareMsgs)

	// Empty the buffer and release the buffer lock.
	node.MsgBuffer.PrepareMsgs = make([]*consensus.VoteMsg, 0)
	node.MsgBuffer.PrepareMsgsMutex.Unlock()

	// Send messages.
	node.MsgDelivery <- msgs
}

func (node *Node) deliveryCommitMsgs() {
	// Copy buffered messages with buffer locked.
	node.MsgBuffer.CommitMsgsMutex.Lock()
	msgs := make([]*consensus.VoteMsg, len(node.MsgBuffer.CommitMsgs))
	copy(msgs, node.MsgBuffer.CommitMsgs)

	// Empty the buffer and release the buffer lock.
	node.MsgBuffer.CommitMsgs = make([]*consensus.VoteMsg, 0)
	node.MsgBuffer.CommitMsgsMutex.Unlock()

	// Send messages.
	node.MsgDelivery <- msgs
}

func (node *Node) resolveMsg() {
	for {
		// Get buffered messages from the dispatcher.
		msgsDelivered := <-node.MsgDelivery
		switch msgs := msgsDelivered.(type) {
		case []*consensus.RequestMsg:
			node.resolveRequestMsg(msgs)
			// Raise alarm to resolve the remained messages
			// in the message buffers.
			node.Alarm <- true
		case []*consensus.PrePrepareMsg:
			node.resolvePrePrepareMsg(msgs)
			// Raise alarm to resolve the remained messages
			// in the message buffers.
			node.Alarm <- true
		case []*consensus.VoteMsg:
			if len(msgs) == 0 {
				break
			}

			if msgs[0].MsgType == consensus.PrepareMsg {
				node.resolvePrepareMsg(msgs)
			} else if msgs[0].MsgType == consensus.CommitMsg {
				node.resolveCommitMsg(msgs)
			}
		}
	}
}

func (node *Node) alarmToDispatcher() {
	for {
		time.Sleep(ResolvingTimeDuration)
		node.Alarm <- true
	}
}

func (node *Node) resolveRequestMsg(msgs []*consensus.RequestMsg) {
	errs := make([]error, 0)

	// Resolve messages
	for _, reqMsg := range msgs {
		err := node.GetReq(reqMsg)
		if err != nil {
			errs = append(errs, err)
			// Send message into dispatcher
			node.MsgEntrance <- reqMsg
		}
	}

	if len(errs) != 0 {
		node.MsgError <- errs
	}
}

func (node *Node) resolvePrePrepareMsg(msgs []*consensus.PrePrepareMsg) {
	errs := make([]error, 0)

	// Resolve messages
	for _, prePrepareMsg := range msgs {
		err := node.GetPrePrepare(prePrepareMsg)
		if err != nil {
			errs = append(errs, err)
			// Send message into dispatcher
			node.MsgEntrance <- prePrepareMsg
		}
	}

	if len(errs) != 0 {
		node.MsgError <- errs
	}
}

func (node *Node) resolvePrepareMsg(msgs []*consensus.VoteMsg) {
	errs := make([]error, 0)

	// Resolve messages
	for _, prepareMsg := range msgs {
		err := node.GetPrepare(prepareMsg)
		if err != nil {
			errs = append(errs, err)
			// Send message into dispatcher
			node.MsgEntrance <- prepareMsg
		}
	}

	if len(errs) != 0 {
		node.MsgError <- errs
	}
}

func (node *Node) resolveCommitMsg(msgs []*consensus.VoteMsg) {
	errs := make([]error, 0)

	// Resolve messages
	for _, commitMsg := range msgs {
		err := node.GetCommit(commitMsg)
		if err != nil {
			errs = append(errs, err)
			// Send message into dispatcher
			node.MsgEntrance <- commitMsg
		}
	}

	if len(errs) != 0 {
		node.MsgError <- errs
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
		case <-time.After(MaxTimeout):
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
