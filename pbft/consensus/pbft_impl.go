package consensus

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type State struct {
	ViewID          int64
	MsgLogs         *MsgLogs
	SequenceID      int64
	CurrentStage    Stage
	CheckPointState int

	// f: the number of Byzantine faulty nodes
	// f = (n-1) / 3
	// e.g., n = 5, f = 1
	F int
}

type MsgLogs struct {
	ReqMsg         *RequestMsg
	PreprepareMsgs *PrePrepareMsg
	PrepareMsgs    map[string]*VoteMsg
	CommitMsgs     map[string]*VoteMsg

	TotalPrepareMsg int32 // atomic
	TotalCommitMsg  int32 // atomic

	PrepareMsgsMutex sync.Mutex
	CommitMsgsMutex  sync.Mutex
	CheckPointMutex  sync.Mutex
}

func CreateState(viewID int64, totNodes int, primaryID string) *State {
	state := &State{
		ViewID: viewID,
		MsgLogs: &MsgLogs{
			ReqMsg:         nil,
			PreprepareMsgs: nil,
			PrepareMsgs:    make(map[string]*VoteMsg),
			CommitMsgs:     make(map[string]*VoteMsg),

			TotalPrepareMsg: 0,
			TotalCommitMsg:  0,
		},
		CurrentStage: Idle,

		F:               (totNodes - 1) / 3,
		CheckPointState: 0,
	}

	// !!!HACK!!!: Primary node does not send the PREPARE message.
	// Add PREPARE pseudo-message from Primary node.
	state.MsgLogs.PrepareMsgs[primaryID] = nil
	atomic.AddInt32(&state.MsgLogs.TotalPrepareMsg, 1)

	return state
}

func (state *State) StartConsensus(request *RequestMsg, sequenceID int64) (*PrePrepareMsg, error) {
	// From TOCS: The primary picks the "ordering" for execution of
	// operations requested by clients. It does this by assigning
	// the next available `sequence number` to a request and sending
	// this assignment to the backups.
	state.SequenceID = sequenceID
	request.SequenceID = sequenceID

	// TODO: From TOCS: no sequence numbers are skipped but
	// when there are view changes some sequence numbers
	// may be assigned to null requests whose execution is a no-op.

	// Save ReqMsgs to its logs.
	state.MsgLogs.ReqMsg = request

	// Get the digest of the request message
	digest, err := Digest(request)
	if err != nil {
		return nil, err
	}

	// Change the stage to pre-prepared.
	state.CurrentStage = PrePrepared

	return &PrePrepareMsg{
		ViewID:     state.ViewID,
		SequenceID: request.SequenceID,
		Digest:     digest,
		RequestMsg: request,
	}, nil
}

func (state *State) PrePrepare(prePrepareMsg *PrePrepareMsg) (*VoteMsg, error) {
	// Get ReqMsgs and save it to its logs like the primary.
	state.MsgLogs.ReqMsg = prePrepareMsg.RequestMsg
	state.MsgLogs.PreprepareMsgs = prePrepareMsg
	// Set sequence number same as PREPREPARE message.
	state.SequenceID = prePrepareMsg.SequenceID

	// Verify if v, n(a.k.a. sequenceID), d are correct.
	if err := state.verifyMsg(prePrepareMsg.ViewID, prePrepareMsg.SequenceID, prePrepareMsg.Digest); err != nil {
		return nil, errors.New("pre-prepare message is corrupted: " + err.Error() + " (operation: " + prePrepareMsg.RequestMsg.Operation + ")")
	}

	// Change the stage to pre-prepared.
	state.CurrentStage = PrePrepared

	return &VoteMsg{
		ViewID:     state.ViewID,
		SequenceID: prePrepareMsg.SequenceID,
		Digest:     prePrepareMsg.Digest,
		MsgType:    PrepareMsg,
	}, nil
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

	// Return nil if the state has already passed prepared stage.
	if int(newTotalPrepareMsg) > 2*state.F {
		return nil, nil
	}

	// Return commit message only once.
	if int(newTotalPrepareMsg) == 2*state.F && state.prepared() {
		// Change the stage to prepared.
		state.CurrentStage = Prepared

		return &VoteMsg{
			ViewID:     state.ViewID,
			SequenceID: prepareMsg.SequenceID,
			Digest:     prepareMsg.Digest,
			MsgType:    CommitMsg,
		}, nil
	}

	return nil, nil
}

func (state *State) Commit(commitMsg *VoteMsg) (*ReplyMsg, *RequestMsg, error) {
	// Check the current state is prepared.
	if !state.prepared() {
		return nil, nil, fmt.Errorf("The stage is not prepared: sequenceID = %d", state.SequenceID)
	}

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
	// Return nil if the state has already passed commited stage.
	if int(newTotalCommitMsg) > 2*state.F {
		return nil, nil, nil
	}
	if int(newTotalCommitMsg) == 2*state.F && state.committed() {
		// Change the stage to committed.
		state.CurrentStage = Committed
		fmt.Printf("[Commit-Vote]: committed. sequence number: %d\n", state.SequenceID)

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

func (state *State) verifyMsg(viewID int64, sequenceID int64, digestGot string) error {
	// Wrong view. That is, wrong configurations of peers to start the consensus.
	if state.ViewID != viewID {
		return fmt.Errorf("state.ViewID = %d, viewID = %d", state.ViewID, viewID)
	}

	if state.SequenceID != sequenceID {
		return fmt.Errorf("state.SequenceID = %d, sequenceID = %d", state.SequenceID, sequenceID)
	}

	digest, err := Digest(state.MsgLogs.ReqMsg)
	if err != nil {
		return err
	}

	// Check digest.
	if digestGot != digest {
		return fmt.Errorf("digest = %s, digestGot = %s", digest, digestGot)
	}

	return nil
}

// From TOCS: Each replica collects messages until it has a quorum certificate
// with the PRE-PREPARE and 2*f matching PREPARE messages for sequence number n,
// view v, and request m. We call this certificate the prepared certificate
// and we say that the replica "prepared" the request.
func (state *State) prepared() bool {
	if state.MsgLogs.ReqMsg == nil {
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

	if int(atomic.LoadInt32(&state.MsgLogs.TotalCommitMsg)) < 2*state.F {
		return false
	}

	return true
}
