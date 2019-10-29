package network

import (
	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
	"fmt"
)

const periodCheckPoint = 5

func (node *Node) GetCheckPoint(CheckPointMsg *consensus.CheckPointMsg) error {
	LogMsg(CheckPointMsg)

	node.CheckPoint(CheckPointMsg)
	return nil
}

func (node *Node) createCheckPointMsg(sequenceID int64, nodeID string) *consensus.CheckPointMsg {
	state, _ := node.getState(sequenceID) // Must succeed.
	digest := state.GetDigest()

	return &consensus.CheckPointMsg{
		SequenceID: sequenceID,
		Digest:     digest,
		NodeID:     nodeID,
	}
}

// Check the CHECKPOINT messages for given sequence number are enough
// including the message for the current node.
func (node *Node) Checkpointchk(state consensus.PBFT) bool {
	node.CheckPointMutex.RLock()
	defer node.CheckPointMutex.RUnlock()

	sequenceID := state.GetSequenceID()
	if len(node.CheckPointMsgsLog[sequenceID]) >= (2*state.GetF() + 1) &&
	   node.CheckPointMsgsLog[sequenceID][node.MyInfo.NodeID] != nil {
		return true
	}

	return false
}

func (node *Node) CheckPoint(msg *consensus.CheckPointMsg) {
	// Save CheckPoint each for Sequence and NodeID.
	node.CheckPointMutex.Lock()
	msgsLog, ok := node.CheckPointMsgsLog[msg.SequenceID]
	if !ok {
		msgsLog = make(map[string]*consensus.CheckPointMsg)
		node.CheckPointMsgsLog[msg.SequenceID] = msgsLog
	}
	msgsLog[msg.NodeID] = msg
	node.CheckPointMutex.Unlock()

	state, _ := node.getState(msg.SequenceID) // Must succeed.

	// Checkpoint only once for each sequence number.
	if node.Checkpointchk(state) && state.GetSuccChkPoint() != 1 {
		fStableCheckPoint := node.StableCheckPoint + periodCheckPoint

		// Delete Checkpoint Message Logs.
		node.CheckPointMutex.Lock()
		for v, _ := range node.CheckPointMsgsLog {
			if int64(v) < fStableCheckPoint {
				delete(node.CheckPointMsgsLog, v)
			}
		}
		node.CheckPointMutex.Unlock()

		// Delete State Message Logs.
		node.StatesMutex.Lock()
		for v, _ := range node.States {
			if int64(v) < fStableCheckPoint {
				delete(node.States, v)
			}
		}
		node.StatesMutex.Unlock()

		// Update checkpoint variables for node and state.
		state.SetSuccChkPoint(1)
		node.StableCheckPoint = fStableCheckPoint
		LogStage("CHECKPOINT", true)
	}

	// Print CheckPoint and MsgLogs.
	if len(msgsLog) == len(node.NodeTable) {
		node.printCheckPoint()
	}
}

// Check the COMMIT messages, for given `periodCheckPoint` consecutive
// sequence numbers, are enough including the messages for the current node.
func (node *Node) CheckPointMissCheck(sequenceID int64) bool {
	for i := (sequenceID + 1); i <= (sequenceID + periodCheckPoint); i++ {
		state, _ := node.getState(i)
		if state == nil {
			return false
		}
		if len(state.GetCommitMsgs()) < (2*state.GetF() + 1) &&
		   state.GetCommitMsgs()[node.MyInfo.NodeID] == nil {
			return false
		}
	}
	return true
}

func (node *Node) printCheckPoint() {
	fmt.Println("CheckPoint History!!")
	node.CheckPointMutex.RLock()
	for v, msgsLog := range node.CheckPointMsgsLog {
		fmt.Println(" Sequence N : ", v)

		for _, j := range msgsLog {
			fmt.Println("    === >", j)
		}
	}
	node.CheckPointMutex.RUnlock()

	fmt.Println("MsgLogs History!!")
	node.StatesMutex.RLock()
	for seqID, state := range node.States {
		fmt.Println(" Sequence N : ", seqID)
		fmt.Println("    === > ReqMsg (digest) : ", state.GetDigest())
		fmt.Println("    === > Preprepare : ", state.GetPrePrepareMsg())
		fmt.Println("    === > Prepare : ", state.GetPrepareMsgs())
		fmt.Println("    === > Commit : ", state.GetCommitMsgs())
	}
	node.StatesMutex.RUnlock()
}
