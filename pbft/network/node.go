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
	MsgEntrance   chan interface{}
	MsgDelivery   chan interface{}
	MsgExecution  chan *MsgPair
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

// Maximum delay between dispatching and delivering messages.
const ResolvingTimeDuration = time.Millisecond * 1000

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

	// TODO:
	// From TOCS: The backups check the sequence numbers assigned by
	// the primary and use timeouts to detect when it stops.
	// They trigger view changes to select a new primary when it
	// appears that the current one has failed.

	return node
}

func (node *Node) Broadcast(msg interface{}, path string) map[string]error {
	errorMap := make(map[string]error)

	for _, nodeInfo := range node.NodeTable {
		if nodeInfo.NodeID == node.NodeID {
			continue
		}

		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			errorMap[nodeInfo.NodeID] = err
			continue
		}

		fmt.Println(nodeInfo)
		err = send(nodeInfo.Url + path, jsonMsg)
		if err != nil {
			errorMap[nodeInfo.NodeID] = err
			continue
		}
	}

	if len(errorMap) == 0 {
		return nil
	} else {
		// TODO: we currently assume all nodes are alive
		//return errorMap
		for nodeID, err := range errorMap {
			fmt.Printf("[%s]: %s\n", nodeID, err)
		}
		panic("Broadcast ERROR!!!")
	}
}

func (node *Node) Reply(msg *consensus.ReplyMsg) error {
	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Client가 없으므로, 일단 Primary에게 보내는 걸로 처리.
	send(node.View.Primary.Url + "/reply", jsonMsg)

	return nil
}

// Consensus start procedure for the Primary.
func (node *Node) GetReq(reqMsg *consensus.RequestMsg) error {
	LogMsg(reqMsg)

	// TODO: Check the request message has a right form.

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
			err := node.routeMsg(msg)
			if err != nil {
				fmt.Println(err)
				// TODO: send err to ErrorChannel
			}
		case <-node.Alarm:
			err := node.routeMsgWhenAlarmed()
			if err != nil {
				fmt.Println(err)
				// TODO: send err to ErrorChannel
			}
		}
	}
}

func (node *Node) routeMsg(msg interface{}) []error {
	switch msg.(type) {
	case *consensus.RequestMsg:
		// Skip if the node is not primary
		if (node.NodeID != node.View.Primary.NodeID) {
			break
		}

		node.MsgBuffer.ReqMsgsMutex.Lock()
		node.MsgBuffer.ReqMsgs = append(node.MsgBuffer.ReqMsgs, msg.(*consensus.RequestMsg))
		node.MsgBuffer.ReqMsgsMutex.Unlock()
	case *consensus.PrePrepareMsg:
		// Skip if the node is primary
		if (node.NodeID == node.View.Primary.NodeID) {
			break
		}

		node.MsgBuffer.PrePrepareMsgsMutex.Lock()
		node.MsgBuffer.PrePrepareMsgs = append(node.MsgBuffer.PrePrepareMsgs, msg.(*consensus.PrePrepareMsg))
		node.MsgBuffer.PrePrepareMsgsMutex.Unlock()
	case *consensus.VoteMsg:
		if msg.(*consensus.VoteMsg).MsgType == consensus.PrepareMsg {
			fmt.Println("Prepare route")
			node.MsgBuffer.PrepareMsgsMutex.Lock()
			node.MsgBuffer.PrepareMsgs = append(node.MsgBuffer.PrepareMsgs, msg.(*consensus.VoteMsg))
			node.MsgBuffer.PrepareMsgsMutex.Unlock()
		} else if msg.(*consensus.VoteMsg).MsgType == consensus.CommitMsg {
			fmt.Println("Commit route")
			node.MsgBuffer.CommitMsgsMutex.Lock()
			node.MsgBuffer.CommitMsgs = append(node.MsgBuffer.CommitMsgs, msg.(*consensus.VoteMsg))
			node.MsgBuffer.CommitMsgsMutex.Unlock()
		}
	}

	return nil
}

func (node *Node) routeMsgWhenAlarmed() []error {
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

	return nil
}

func (node *Node) deliveryRequestMsgs() []error {
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

	return nil
}

func (node *Node) deliveryPrePrepareMsgs() []error {
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

	return nil
}

func (node *Node) deliveryPrepareMsgs() []error {
	// Copy buffered messages with buffer locked.
	node.MsgBuffer.PrepareMsgsMutex.Lock()
	msgs := make([]*consensus.VoteMsg, len(node.MsgBuffer.PrepareMsgs))
	copy(msgs, node.MsgBuffer.PrepareMsgs)

	// Empty the buffer and release the buffer lock.
	node.MsgBuffer.PrepareMsgs = make([]*consensus.VoteMsg, 0)
	node.MsgBuffer.PrepareMsgsMutex.Unlock()

	// Send messages.
	node.MsgDelivery <- msgs

	return nil
}

func (node *Node) deliveryCommitMsgs() []error {
	// Copy buffered messages with buffer locked.
	node.MsgBuffer.CommitMsgsMutex.Lock()
	msgs := make([]*consensus.VoteMsg, len(node.MsgBuffer.CommitMsgs))
	copy(msgs, node.MsgBuffer.CommitMsgs)

	// Empty the buffer and release the buffer lock.
	node.MsgBuffer.CommitMsgs = make([]*consensus.VoteMsg, 0)
	node.MsgBuffer.CommitMsgsMutex.Unlock()

	// Send messages.
	node.MsgDelivery <- msgs

	return nil
}

func (node *Node) resolveMsg() {
	for {
		// Get buffered messages from the dispatcher.
		msgs := <-node.MsgDelivery
		switch msgs.(type) {
		case []*consensus.RequestMsg:
			errs := node.resolveRequestMsg(msgs.([]*consensus.RequestMsg))
			if len(errs) != 0 {
				for _, err := range errs {
					fmt.Println(err)
				}
				// TODO: send err to ErrorChannel
			}
		case []*consensus.PrePrepareMsg:
			errs := node.resolvePrePrepareMsg(msgs.([]*consensus.PrePrepareMsg))
			if len(errs) != 0 {
				for _, err := range errs {
					fmt.Println(err)
				}
				// TODO: send err to ErrorChannel
			}
		case []*consensus.VoteMsg:
			voteMsgs := msgs.([]*consensus.VoteMsg)
			if len(voteMsgs) == 0 {
				break
			}

			if voteMsgs[0].MsgType == consensus.PrepareMsg {
				errs := node.resolvePrepareMsg(voteMsgs)
				if len(errs) != 0 {
					for _, err := range errs {
						fmt.Println(err)
					}
					// TODO: send err to ErrorChannel
				}
			} else if voteMsgs[0].MsgType == consensus.CommitMsg {
				errs := node.resolveCommitMsg(voteMsgs)
				if len(errs) != 0 {
					for _, err := range errs {
						fmt.Println(err)
					}
					// TODO: send err to ErrorChannel
				}
			}
		}

		// Raise alarm that some messages may be buffered.
		node.Alarm <- true
	}
}

func (node *Node) alarmToDispatcher() {
	for {
		time.Sleep(ResolvingTimeDuration)
		node.Alarm <- true
	}
}

func (node *Node) resolveRequestMsg(msgs []*consensus.RequestMsg) []error {
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
		return errs
	}

	return nil
}

func (node *Node) resolvePrePrepareMsg(msgs []*consensus.PrePrepareMsg) []error {
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
		return errs
	}

	return nil
}

func (node *Node) resolvePrepareMsg(msgs []*consensus.VoteMsg) []error {
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
		return errs
	}

	return nil
}

func (node *Node) resolveCommitMsg(msgs []*consensus.VoteMsg) []error {
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
		return errs
	}

	return nil
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

		for sequenceID, p := range pairs {
			var lastCommittedMsg *consensus.RequestMsg

			// Find the last committed message.
			msgTotalCnt := len(node.CommittedMsgs)
			if msgTotalCnt > 0 {
				lastCommittedMsg = node.CommittedMsgs[msgTotalCnt - 1]
			}

			// Stop execution if the timely message does not executed.
			if lastCommittedMsg != nil &&
			   lastCommittedMsg.SequenceID + 1 != sequenceID {
				break
			}

			committedMsgs = append(committedMsgs, p.committedMsg)
			LogStage("Commit", true)

			// TODO: execute appropriate operation.
			p.replyMsg.Result = "Executed"

			// Save the last version of committed messages to node.
			node.CommittedMsgs = append(node.CommittedMsgs, p.committedMsg)

			node.Reply(p.replyMsg)
			LogStage("Reply", true)

			// Delete the current message pair.
			// [Golang: If a map entry is created during iteration,
			// that entry may be produced during the iteration
			// or may be skipped.]
			delete(pairs, sequenceID)
		}

		// Print all committed messages.
		for _, v := range committedMsgs {
			fmt.Printf("Committed value: %s, %d, %s, %d\n",
			            v.ClientID, v.Timestamp, v.Operation, v.SequenceID)
		}
	}
}

func (node *Node) startConsensus(state consensus.PBFT, reqMsg *consensus.RequestMsg) (*consensus.PrePrepareMsg, error) {
	return state.StartConsensus(reqMsg)
}

func (node *Node) prePrepare(state consensus.PBFT, prePrepareMsg *consensus.PrePrepareMsg) (*consensus.VoteMsg, error) {
	return state.PrePrepare(prePrepareMsg)
}

func (node *Node) prepare(state consensus.PBFT, prepareMsg *consensus.VoteMsg) (*consensus.VoteMsg, error) {
	stage := state.GetStage()
	if stage != consensus.PrePrepared {
		return nil, fmt.Errorf("Current Stage (seqID: %d) is %d, not pre-prepared.", prepareMsg.SequenceID, stage)
	}

	return state.Prepare(prepareMsg)
}

func (node *Node) commit(state consensus.PBFT, commitMsg *consensus.VoteMsg) (*consensus.ReplyMsg, *consensus.RequestMsg, error) {
	stage := state.GetStage()
	if stage != consensus.Prepared {
		return nil, nil, fmt.Errorf("Current Stage (seqID: %d) is %d, not prepared.", commitMsg.SequenceID, stage)
	}

	return state.Commit(commitMsg)
}

func (node *Node) updateView(viewID int64) {
	node.View.ID = viewID
	node.View.LastSequenceID = 0
	viewIdx := viewID % int64(len(node.NodeTable))
	node.View.Primary = node.NodeTable[viewIdx]

	fmt.Println("ViewID:", node.View.ID, "Primary:", node.View.Primary.NodeID)
}
