package consensus

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type State struct {
	ViewID          int64
	NodeID          string
	MsgLogs         *MsgLogs
	SequenceID      int64

	MsgState chan interface{}

	// f: the number of Byzantine faulty nodes
	// f = (n-1) / 3
	// e.g., n = 5, f = 1
	F int

	// Cache the invariant digest of ReqMsg for each sequence ID.
	digest string

	// checkpointdelete check
	succChkPointDelete int64
}

type MsgLogs struct {
	ReqMsg        *RequestMsg
	PrePrepareMsg *PrePrepareMsg
	PrepareMsgs   map[string]*VoteMsg
	CommitMsgs    map[string]*VoteMsg

	PrepareMsgsMutex sync.RWMutex
	CommitMsgsMutex  sync.RWMutex

	// Count PREPARE message created from the current node
	// as one PREPARE message. PRE-PREPARE message from
	// primary node is also regarded as PREPARE message but
	// do not count it, because it is not real PREPARE message.
	// Count COMMIT message created from the current node
	// as one COMMIT message.
	TotalPrepareMsg  int32 // atomic
	TotalCommitMsg   int32 // atomic

	// Flags whether each message has broadcasted.
	// Its value is atomically swapped by CompareAndSwapInt32().
	commitMsgSent   int32 // atomic bool
	replyMsgSent    int32 // atomic bool
}

func CreateState(viewID int64, nodeID string, totNodes int) *State {
	state := &State{
		ViewID: viewID,
		NodeID: nodeID,
		MsgLogs: &MsgLogs{
			ReqMsg:        nil,
			PrePrepareMsg: nil,
			PrepareMsgs:   make(map[string]*VoteMsg),
			CommitMsgs:    make(map[string]*VoteMsg),

			// Setting these counters during consensus is unsafe
			// because quorum condition check can be skipped.
			TotalPrepareMsg: 0,
			TotalCommitMsg: 0,

			commitMsgSent: 0,
			replyMsgSent: 0,
		},
		MsgState: make(chan interface{}, totNodes), // stack enough

		F: (totNodes - 1) / 3,
		succChkPointDelete: 0,
	}

	return state
}

func (state *State) StartConsensus(request *RequestMsg, sequenceID int64) *PrePrepareMsg {
	// From TOCS: The primary picks the "ordering" for execution of
	// operations requested by clients. It does this by assigning
	// the next available `sequence number` to a request and sending
	// this assignment to the backups.
	state.SequenceID = sequenceID
	request.SequenceID = sequenceID

	// TODO: From TOCS: no sequence numbers are skipped but
	// when there are view changes some sequence numbers
	// may be assigned to null requests whose execution is a no-op.

	// Log REQUEST message.
	state.MsgLogs.ReqMsg = request

	// Get the digest of the request message
	state.digest = Digest(request)

	// Create PREPREPARE message.
	prePrepareMsg := &PrePrepareMsg{
		ViewID:     state.ViewID,
		SequenceID: request.SequenceID,
		Digest:     state.digest,
	}

	// Accessing to the message log without locking is safe because
	// nobody except for this node starts consensus at this point,
	// i.e., the node has not registered this state yet.
	state.MsgLogs.PrePrepareMsg = prePrepareMsg

	return prePrepareMsg
}

func (state *State) PrePrepare(prePrepareMsg *PrePrepareMsg) (*VoteMsg, error) {
	// Log PREPREPARE message.
	state.MsgLogs.PrePrepareMsg = prePrepareMsg

	// Set sequence number same as PREPREPARE message sent from Primary.
	state.SequenceID = prePrepareMsg.SequenceID

	// Verify if v, n(a.k.a. sequenceID), d are correct.
	if err := state.verifyMsg(prePrepareMsg.ViewID, prePrepareMsg.SequenceID, prePrepareMsg.Digest); err != nil {
		return nil, errors.New("pre-prepare message is corrupted: " + err.Error() + " (sequenceID: " + fmt.Sprintf("%d", prePrepareMsg.SequenceID) + ")")
	}

	// Create PREPARE message.
	prepareMsg := &VoteMsg{
		ViewID:     state.ViewID,
		SequenceID: prePrepareMsg.SequenceID,
		Digest:     prePrepareMsg.Digest,
		MsgType:    PrepareMsg,
	}

	return prepareMsg, nil
}

func (state *State) Prepare(prepareMsg *VoteMsg) (*VoteMsg, error) {
	if err := state.verifyMsg(prepareMsg.ViewID, prepareMsg.SequenceID, prepareMsg.Digest); err != nil {
		return nil, errors.New("prepare message is corrupted: " + err.Error() + " (nodeID: " + prepareMsg.NodeID + ")")
	}

	// Append msg to its logs
	state.MsgLogs.PrepareMsgsMutex.Lock()
	if _, ok := state.MsgLogs.PrepareMsgs[prepareMsg.NodeID]; ok {
		fmt.Printf("Prepare message from %s is already received, sequence number=%d\n",
		           prepareMsg.NodeID, state.SequenceID)
		state.MsgLogs.PrepareMsgsMutex.Unlock()
		return nil, nil
	}
	state.MsgLogs.PrepareMsgs[prepareMsg.NodeID] = prepareMsg
	state.MsgLogs.PrepareMsgsMutex.Unlock()
	newTotalPrepareMsg := atomic.AddInt32(&state.MsgLogs.TotalPrepareMsg, 1)

	// Print current voting status
	fmt.Printf("[Prepare-Vote]: %d, from %s, sequence number: %d\n",
	           newTotalPrepareMsg, prepareMsg.NodeID, prepareMsg.SequenceID)

	// Return commit message only once.
	if int(newTotalPrepareMsg) >= 2*state.F && state.prepared() &&
	   atomic.CompareAndSwapInt32(&state.MsgLogs.commitMsgSent, 0, 1) {
		// Create COMMIT message.
		commitMsg := &VoteMsg{
			ViewID:     state.ViewID,
			SequenceID: prepareMsg.SequenceID,
			Digest:     prepareMsg.Digest,
			MsgType:    CommitMsg,
		}

		return commitMsg, nil
	}

	return nil, nil
}

func (state *State) Commit(commitMsg *VoteMsg) (*ReplyMsg, *RequestMsg, error) {
	if err := state.verifyMsg(commitMsg.ViewID, commitMsg.SequenceID, commitMsg.Digest); err != nil {
		return nil, nil, errors.New("commit message is corrupted: " + err.Error() + " (nodeID: " + commitMsg.NodeID + ")")
	}

	// Append msg to its logs
	state.MsgLogs.CommitMsgsMutex.Lock()
	if _, ok := state.MsgLogs.CommitMsgs[commitMsg.NodeID]; ok {
		fmt.Printf("Commit message from %s is already received, sequence number=%d\n",
		           commitMsg.NodeID, state.SequenceID)
		state.MsgLogs.CommitMsgsMutex.Unlock()
		return nil, nil, nil
	}
	state.MsgLogs.CommitMsgs[commitMsg.NodeID] = commitMsg
	state.MsgLogs.CommitMsgsMutex.Unlock()
	newTotalCommitMsg := atomic.AddInt32(&state.MsgLogs.TotalCommitMsg, 1)

	// Print current voting status
	fmt.Printf("[Commit-Vote]: %d, from %s, sequence number: %d\n",
	           newTotalCommitMsg, commitMsg.NodeID, commitMsg.SequenceID)

	// Return reply message only once.
	if int(newTotalCommitMsg) >= 2*state.F + 1 && state.committed() &&
	   atomic.CompareAndSwapInt32(&state.MsgLogs.replyMsgSent, 0, 1) {
		fmt.Printf("[Commit-Vote]: committed. sequence number: %d\n", state.SequenceID)
		request := state.MsgLogs.ReqMsg
		if request == nil {
			return nil, nil, errors.New("Reply message created but committed message is nil")
		}

		return &ReplyMsg{
			ViewID:    state.ViewID,
			Timestamp: state.MsgLogs.ReqMsg.Timestamp,
			ClientID:  state.MsgLogs.ReqMsg.ClientID,
			// Nodes must execute the requested operation
			// locally and assign the result into reply message,
			// with considering their operation ordering policy.
			Result: "",
		}, state.MsgLogs.ReqMsg, nil
	}

	return nil, nil, nil
}

func (state *State) GetSequenceID() int64 {
	return state.SequenceID
}

func (state *State) GetDigest() string {
	return state.digest
}

func (state *State) GetF() int {
	return state.F
}

func (state *State) GetMsgReceiveChannel() <-chan interface{} {
	return state.MsgState
}

func (state *State) GetMsgSendChannel() chan<- interface{} {
	return state.MsgState
}

func (state *State) GetReqMsg() *RequestMsg {
	return state.MsgLogs.ReqMsg
}

func (state *State) GetPrePrepareMsg() *PrePrepareMsg {
	return state.MsgLogs.PrePrepareMsg
}

func (state *State) GetSuccChkPoint() int64 {
	return state.succChkPointDelete
}

func (state *State) SetSuccChkPoint(num int64) {
	state.succChkPointDelete = num
}

func (state *State) SetReqMsg(request *RequestMsg) {
	state.MsgLogs.ReqMsg = request
}

func (state *State) SetPrePrepareMsg(prePrepareMsg *PrePrepareMsg) {
	state.MsgLogs.PrePrepareMsg = prePrepareMsg
}

func (state *State) SetSequenceID(sequenceID int64) {
	state.SequenceID = sequenceID
}

func (state *State) SetDigest(digest string) {
	state.digest = digest
}

func (state *State) SetViewID(viewID int64) {
	state.ViewID = viewID
}

func (state *State) GetPrepareMsgs() map[string]*VoteMsg {
	newMap := make(map[string]*VoteMsg)

	state.MsgLogs.PrepareMsgsMutex.RLock()
	for k, v := range state.MsgLogs.PrepareMsgs {
		newMap[k] = v
	}
	state.MsgLogs.PrepareMsgsMutex.RUnlock()

	return newMap
}

func (state *State) GetCommitMsgs() map[string]*VoteMsg {
	newMap := make(map[string]*VoteMsg)

	state.MsgLogs.CommitMsgsMutex.RLock()
	for k, v := range state.MsgLogs.CommitMsgs {
		newMap[k] = v
	}
	state.MsgLogs.CommitMsgsMutex.RUnlock()

	return newMap
}

func (state *State) verifyMsg(viewID int64, sequenceID int64, digestGot string) error {
	// Wrong view. That is, wrong configurations of peers to start the consensus.
	if state.ViewID != viewID {
		return fmt.Errorf("state.ViewID = %d, viewID = %d", state.ViewID, viewID)
	}

	if state.SequenceID != sequenceID {
		return fmt.Errorf("state.SequenceID = %d, sequenceID = %d", state.SequenceID, sequenceID)
	}

	// Check digest.
	if state.digest != digestGot {
		return fmt.Errorf("state.digest = %s, digestGot = %s", state.digest, digestGot)
	}

	return nil
}

// From TOCS: Each replica collects messages until it has a quorum certificate
// with the PRE-PREPARE and 2*f matching PREPARE messages for sequence number n,
// view v, and request m. We call this certificate the prepared certificate
// and we say that the replica "prepared" the request.
func (state *State) prepared() bool {
	if state.MsgLogs.ReqMsg == nil || state.MsgLogs.PrePrepareMsg == nil {
		return false
	}

	if int(atomic.LoadInt32(&state.MsgLogs.TotalPrepareMsg)) < 2*state.F {
		return false
	}

	return true
}

// From TOCS: Each replica collects messages until it has a quorum certificate
// with 2*f+1 COMMIT messages for the same sequence number n and view v
// from different replicas (including itself). We call this certificate
// the committed certificate and say that the request is "committed"
// by the replica when it has both the prepared and committed certificates.
func (state *State) committed() bool {
	if !state.prepared() {
		return false
	}

	if int(atomic.LoadInt32(&state.MsgLogs.TotalCommitMsg)) < 2*state.F + 1 {
		return false
	}

	return true
}