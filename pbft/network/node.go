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
	NodeTable     map[string]string // key=nodeID, value=url
	View          *View
	CurrentState  *consensus.State
	CommittedMsgs []*consensus.RequestMsg // kinda block.
	MsgBuffer     *MsgBuffer
	MsgEntrance   chan interface{}
	MsgDelivery   chan interface{}
	Alarm         chan bool
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
	ID      int64
	Primary string
}

const ResolvingTimeDuration = time.Millisecond * 1000 // 1 second.

func NewNode(nodeID string) *Node {
	const viewID = 10000000000 // temporary.

	var nodeTable = map[string]string{
		"Apple": "localhost:1111",
		"MS": "localhost:1112",
		"Google": "localhost:1113",
		"IBM": "localhost:1114",
	}

	// Hard-coded for test.
	node := &Node{
		NodeID: nodeID,
		NodeTable: nodeTable,
		View: &View{
			ID: viewID,
			Primary: "Apple",
		},

		// Consensus-related struct
		CurrentState: nil,
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
		Alarm: make(chan bool, 1),
	}

	// Start message dispatcher
	go node.dispatchMsg()

	// Start alarm trigger
	go node.alarmToDispatcher()

	// Start message resolver
	go node.resolveMsg()

	// TODO:
	// From TOCS: The backups check the sequence numbers assigned by
	// the primary and use timeouts to detect when it stops.
	// They trigger view changes to select a new primary when it
	// appears that the current one has failed.

	return node
}

func (node *Node) Broadcast(msg interface{}, path string) map[string]error {
	errorMap := make(map[string]error)

	for nodeID, url := range node.NodeTable {
		if nodeID == node.NodeID {
			continue
		}

		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			errorMap[nodeID] = err
			continue
		}

		fmt.Println(nodeID)
		err = send(url + path, jsonMsg)
		if err != nil {
			errorMap[nodeID] = err
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
	// Print all committed messages.
	for _, value := range node.CommittedMsgs {
		fmt.Printf("Committed value: %s, %d, %s, %d", value.ClientID, value.Timestamp, value.Operation, value.SequenceID)
	}
	fmt.Print("\n")

	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Client가 없으므로, 일단 Primary에게 보내는 걸로 처리.
	send(node.NodeTable[node.View.Primary] + "/reply", jsonMsg)

	// Notify the node can handle the next request messages.
	node.CurrentState = nil

	return nil
}

// GetReq can be called when the node's CurrentState is nil.
// Consensus start procedure for the Primary.
func (node *Node) GetReq(reqMsg *consensus.RequestMsg) error {
	LogMsg(reqMsg)

	// TODO: Check the request message has a right form.

	// Create a new state for the new consensus.
	err := node.createStateForNewConsensus(reqMsg.Timestamp)
	if err != nil {
		return err
	}

	// Start the consensus process.
	prePrepareMsg, err := node.CurrentState.StartConsensus(reqMsg)
	if err != nil {
		return err
	}

	LogStage(fmt.Sprintf("Consensus Process (ViewID:%d)", node.CurrentState.ViewID), false)

	// Send getPrePrepare message
	if prePrepareMsg != nil {
		node.Broadcast(prePrepareMsg, "/preprepare")
		LogStage("Pre-prepare", true)
	}

	return nil
}

// GetPrePrepare can be called when the node's CurrentState is nil.
// Consensus start procedure for normal participants.
func (node *Node) GetPrePrepare(prePrepareMsg *consensus.PrePrepareMsg) error {
	LogMsg(prePrepareMsg)

	// Create a new state for the new consensus.
	err := node.createStateForNewConsensus(prePrepareMsg.RequestMsg.Timestamp)
	if err != nil {
		return err
	}

	prePareMsg, err := node.CurrentState.PrePrepare(prePrepareMsg)
	if err != nil {
		return err
	}

	if prePareMsg != nil {
		// Attach node ID to the message
		prePareMsg.NodeID = node.NodeID

		LogStage("Pre-prepare", true)
		node.Broadcast(prePareMsg, "/prepare")
		LogStage("Prepare", false)

		// !!!HACK!!!: Add PREPARE pseudo-message from Primary node
		// because Primary node does not send the PREPARE message.
		node.CurrentState.MsgLogs.PrepareMsgs[node.View.Primary] = nil
	}

	return nil
}

func (node *Node) GetPrepare(prepareMsg *consensus.VoteMsg) error {
	LogMsg(prepareMsg)

	commitMsg, err := node.CurrentState.Prepare(prepareMsg)
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

	replyMsg, committedMsg, err := node.CurrentState.Commit(commitMsg)
	if err != nil {
		return err
	}

	if replyMsg != nil {
		if committedMsg == nil {
			return errors.New("committed message is nil, even though the reply message is not nil")
		}

		// Attach node ID to the message
		replyMsg.NodeID = node.NodeID

		// Save the last version of committed messages to node.
		node.CommittedMsgs = append(node.CommittedMsgs, committedMsg)

		LogStage("Commit", true)
		node.Reply(replyMsg)
		LogStage("Reply", true)
	}

	return nil
}

func (node *Node) GetReply(msg *consensus.ReplyMsg) {
	fmt.Printf("Result: %s by %s\n", msg.Result, msg.NodeID)
}

func (node *Node) createStateForNewConsensus(timeStamp int64) error {
	// Check if there is an ongoing consensus process.
	if node.CurrentState != nil {
		return errors.New("Discard Requests: another consensus is ongoing")
	}

	// Get the last sequence ID
	var lastSequenceID int64
	if len(node.CommittedMsgs) == 0 {
		lastSequenceID = -1
	} else {
		// From TOCS: To guarantee exactly once semantics,
		// replicas discard requests whose timestamp is lower than
		// the timestamp in the last reply they sent to the client.
		lastCommitMsg := node.CommittedMsgs[len(node.CommittedMsgs) - 1]
/*
		if timeStamp <= lastCommitMsg.Timestamp {
			// TODO: Delete the messages efficiently.
			return errors.New("Discard Requests: old request")
		}
*/
		lastSequenceID = lastCommitMsg.SequenceID
	}

	// Create a new state for this new consensus process in the Primary
	node.CurrentState = consensus.CreateState(node.View.ID, lastSequenceID)

	LogStage("Create the replica status", true)

	return nil
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
		node.MsgBuffer.ReqMsgsMutex.Lock()
		node.MsgBuffer.ReqMsgs = append(node.MsgBuffer.ReqMsgs, msg.(*consensus.RequestMsg))
		node.MsgBuffer.ReqMsgsMutex.Unlock()
		if node.CurrentState == nil {
			node.deliveryRequestMsgs()
		}
	case *consensus.PrePrepareMsg:
		node.MsgBuffer.PrePrepareMsgsMutex.Lock()
		node.MsgBuffer.PrePrepareMsgs = append(node.MsgBuffer.PrePrepareMsgs, msg.(*consensus.PrePrepareMsg))
		node.MsgBuffer.PrePrepareMsgsMutex.Unlock()
		if node.CurrentState == nil {
			node.deliveryPrePrepareMsgs()
		}
	case *consensus.VoteMsg:
		if msg.(*consensus.VoteMsg).MsgType == consensus.PrepareMsg {
			fmt.Println("Prepare route")
			node.MsgBuffer.PrepareMsgsMutex.Lock()
			node.MsgBuffer.PrepareMsgs = append(node.MsgBuffer.PrepareMsgs, msg.(*consensus.VoteMsg))
			node.MsgBuffer.PrepareMsgsMutex.Unlock()
			if node.CurrentState != nil && node.CurrentState.CurrentStage == consensus.PrePrepared {
				node.deliveryPrepareMsgs()
			}
		} else if msg.(*consensus.VoteMsg).MsgType == consensus.CommitMsg {
			fmt.Println("Commit route")
			node.MsgBuffer.CommitMsgsMutex.Lock()
			node.MsgBuffer.CommitMsgs = append(node.MsgBuffer.CommitMsgs, msg.(*consensus.VoteMsg))
			node.MsgBuffer.CommitMsgsMutex.Unlock()
			if node.CurrentState != nil && node.CurrentState.CurrentStage == consensus.Prepared {
				node.deliveryCommitMsgs()
			}
		}
	}

	return nil
}

func (node *Node) routeMsgWhenAlarmed() []error {
	if node.CurrentState == nil {
		// Check ReqMsgs, send them.
		if len(node.MsgBuffer.ReqMsgs) != 0 {
			node.deliveryRequestMsgs()
		}

		// Check PrePrepareMsgs, send them.
		if len(node.MsgBuffer.PrePrepareMsgs) != 0 {
			node.deliveryPrePrepareMsgs()
		}
	} else {
		switch node.CurrentState.CurrentStage {
		case consensus.PrePrepared:
			// Check PrepareMsgs, send them.
			if len(node.MsgBuffer.PrepareMsgs) != 0 {
				node.deliveryPrepareMsgs()
			}
		case consensus.Prepared:
			// Check CommitMsgs, send them.
			if len(node.MsgBuffer.CommitMsgs) != 0 {
				node.deliveryCommitMsgs()
			}
		}
	}

	return nil
}

func (node *Node) deliveryRequestMsgs() []error {
	// Copy buffered messages with buffer locked.
	node.MsgBuffer.ReqMsgsMutex.Lock()
	msgs := make([]*consensus.RequestMsg, len(node.MsgBuffer.ReqMsgs))
	copy(msgs, node.MsgBuffer.ReqMsgs)

	// Empty the buffer and release the buffer lock.
	node.MsgBuffer.ReqMsgs = make([]*consensus.RequestMsg, 0)
	node.MsgBuffer.ReqMsgsMutex.Unlock()

	// Send messages.
	node.MsgDelivery <- msgs

	return nil
}

func (node *Node) deliveryPrePrepareMsgs() []error {
	// Copy buffered messages with buffer locked.
	node.MsgBuffer.PrePrepareMsgsMutex.Lock()
	msgs := make([]*consensus.PrePrepareMsg, len(node.MsgBuffer.PrePrepareMsgs))
	copy(msgs, node.MsgBuffer.PrePrepareMsgs)

	// Empty the buffer and release the buffer lock.
	node.MsgBuffer.PrePrepareMsgs = make([]*consensus.PrePrepareMsg, 0)
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
