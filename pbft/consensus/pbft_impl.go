package consensus

import (
	"encoding/json"
	"errors"
	"fmt"
)

type State struct {
	ViewID         int64
	MsgLogs        *MsgLogs
	LastSequenceID int64
	CurrentStage   Stage
}

type MsgLogs struct {
	ReqMsg        *RequestMsg
	PrepareMsgs   map[string]*VoteMsg
	CommitMsgs    map[string]*VoteMsg
}

// f: # of Byzantine faulty node
// f = (n-1) / 3
// n = 4, in this case.
const f = 1

// lastSequenceID will be -1 if there is no last sequence ID.
func CreateState(viewID int64, lastSequenceID int64) *State {
	return &State{
		ViewID: viewID,
		MsgLogs: &MsgLogs{
			ReqMsg:nil,
			PrepareMsgs:make(map[string]*VoteMsg),
			CommitMsgs:make(map[string]*VoteMsg),
		},
		LastSequenceID: lastSequenceID,
		CurrentStage: Idle,
	}
}

func (state *State) StartConsensus(request *RequestMsg) (*PrePrepareMsg, error) {
	// From TOCS: The primary picks the "ordering" for execution of
	// operations requested by clients. It does this by assigning
	// the next available `sequence number` to a request and sending
	// this assignment to the backups.
	request.SequenceID = state.LastSequenceID + 1

	// TODO: From TOCS: no sequence numbers are skipped but
	// when there are view changes some sequence numbers
	// may be assigned to null requests whose execution is a no-op.

	// Save ReqMsgs to its logs.
	state.MsgLogs.ReqMsg = request

	// Get the digest of the request message
	digest, err := digest(request)
	if err != nil {
		return nil, err
	}

	// Change the stage to pre-prepared.
	state.CurrentStage = PrePrepared

	return &PrePrepareMsg{
		ViewID: state.ViewID,
		SequenceID: request.SequenceID,
		Digest: digest,
		RequestMsg: request,
	}, nil
}

func (state *State) PrePrepare(prePrepareMsg *PrePrepareMsg) (*VoteMsg, error) {
	// Get ReqMsgs and save it to its logs like the primary.
	state.MsgLogs.ReqMsg = prePrepareMsg.RequestMsg

	// Verify if v, n(a.k.a. sequenceID), d are correct.
	if err := state.verifyMsg(prePrepareMsg.ViewID, prePrepareMsg.SequenceID, prePrepareMsg.Digest); err != nil {
		return nil, errors.New("pre-prepare message is corrupted: " + err.Error() + " (operation: " + prePrepareMsg.RequestMsg.Operation + ")")
	}

	// Change the stage to pre-prepared.
	state.CurrentStage = PrePrepared

	return &VoteMsg{
		ViewID: state.ViewID,
		SequenceID: prePrepareMsg.SequenceID,
		Digest: prePrepareMsg.Digest,
		MsgType: PrepareMsg,
	}, nil
}


func (state *State) Prepare(prepareMsg *VoteMsg) (*VoteMsg, error){
	if err := state.verifyMsg(prepareMsg.ViewID, prepareMsg.SequenceID, prepareMsg.Digest); err != nil {
		return nil, errors.New("prepare message is corrupted: " + err.Error() + " (nodeID: " + prepareMsg.NodeID + ")")
	}

	// Append msg to its logs
	state.MsgLogs.PrepareMsgs[prepareMsg.NodeID] = prepareMsg

	// Print current voting status
	fmt.Printf("[Prepare-Vote]: %d\n", len(state.MsgLogs.PrepareMsgs))

	// Return nil if the state has already passed prepared stage.
	if len(state.MsgLogs.PrepareMsgs) > 2*f {
		return nil, nil
	}

	if state.prepared() {
		// Change the stage to prepared.
		state.CurrentStage = Prepared

		return &VoteMsg{
			ViewID: state.ViewID,
			SequenceID: prepareMsg.SequenceID,
			Digest: prepareMsg.Digest,
			MsgType: CommitMsg,
		}, nil
	}

	return nil, nil
}

func (state *State) Commit(commitMsg *VoteMsg) (*ReplyMsg, *RequestMsg, error) {
	if err := state.verifyMsg(commitMsg.ViewID, commitMsg.SequenceID, commitMsg.Digest); err != nil {
		return nil, nil, errors.New("commit message is corrupted: " + err.Error() + " (nodeID: " + commitMsg.NodeID + ")")
	}

	// Append msg to its logs
	state.MsgLogs.CommitMsgs[commitMsg.NodeID] = commitMsg

	// Print current voting status
	fmt.Printf("[Commit-Vote]: %d\n", len(state.MsgLogs.CommitMsgs))

	// Return nil if the state has already passed commited stage.
	if len(state.MsgLogs.CommitMsgs) > 2*f {
		return nil, nil, nil
	}

	if state.committed() {
		// Change the stage to prepared.
		state.CurrentStage = Committed

		return &ReplyMsg{
			ViewID: state.ViewID,
			Timestamp: state.MsgLogs.ReqMsg.Timestamp,
			ClientID: state.MsgLogs.ReqMsg.ClientID,
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

	// Check if the Primary sent fault sequence number. => Faulty primary.
	// TODO: adopt upper/lower bound check.
	if state.LastSequenceID != -1 {
		if state.LastSequenceID >= sequenceID {
			return fmt.Errorf("state.LastSequenceID = %d, sequenceID = %d", state.LastSequenceID, sequenceID)
		}
	}

	digest, err := digest(state.MsgLogs.ReqMsg)
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
// TODO: async protocol
func (state *State) prepared() bool {
	if state.MsgLogs.ReqMsg == nil {
		return false
	}

	if len(state.MsgLogs.PrepareMsgs) < 2*f {
		return false
	}

	return true
}

// From TOCS: Each replica collects messages until it has a quorum certificate
// with 2*f+1 COMMIT messages for the same sequence number n and view v
// from different replicas (including itself). We call this certificate
// the committed certificate and say that the request is "committed"
// by the replica when it has both the prepared and committed certificates.
// TODO: async protocol
func (state *State) committed() bool {
	if !state.prepared() {
		return false
	}

	if len(state.MsgLogs.CommitMsgs) < 2*f {
		return false
	}

	return true
}

func digest(object interface{}) (string, error) {
	msg, err := json.Marshal(object)

	if err != nil {
		return "", err
	}

	return Hash(msg), nil
}
