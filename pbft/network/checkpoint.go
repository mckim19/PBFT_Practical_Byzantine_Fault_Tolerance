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

func (node *Node) getCheckPointMsg(SequenceID int64, nodeID string, ReqMsgs *consensus.RequestMsg) (*consensus.CheckPointMsg, error) {
	state, err := node.getState(SequenceID)
	if err != nil {
		return nil, err
	}
	digest := state.GetDigest()

	return &consensus.CheckPointMsg{
		SequenceID: SequenceID,
		Digest:     digest,
		NodeID:     nodeID,
	}, nil
}

func (node *Node) Checkpointchk(sequenceID int64) bool {
	state, _ := node.getState(sequenceID)
	if state == nil {
		return false
	}

	if len(node.CheckPointMsgsLog[sequenceID]) >= (2*state.GetF() + 1) &&
	   node.CheckPointMsgsLog[sequenceID][node.MyInfo.NodeID] != nil {
		return true
	}

	return false
}
func (node *Node) CheckPoint(msg *consensus.CheckPointMsg) {
	// Test and test-and-set scheme.
	msgsLog, ok := node.CheckPointMsgsLog[msg.SequenceID]
	if !ok {
		// TODO: do not use state lock.
		node.StatesMutex.Lock()
		msgsLog, ok = node.CheckPointMsgsLog[msg.SequenceID]
		if !ok {
			msgsLog = make(map[string]*consensus.CheckPointMsg)
			node.CheckPointMsgsLog[msg.SequenceID] = msgsLog
		}
		node.StatesMutex.Unlock()
	}

	// Save CheckPoint each for Sequence and NodeID.
	msgsLog[msg.NodeID] = msg

	// Checkpoint only once for each sequence number.
	if node.Checkpointchk(msg.SequenceID) && !ok {
		fStableCheckPoint := node.StableCheckPoint + periodCheckPoint
		// Delete Checkpoint Message Logs
		for v, _ := range node.CheckPointMsgsLog {
			if int64(v) < fStableCheckPoint {
				delete(node.CheckPointMsgsLog, v)
			}
		}
		// Delete State Message Logs
		node.StatesMutex.Lock()
		for v, _ := range node.States {
			if int64(v) < fStableCheckPoint {
				delete(node.States, v)
			}
		}
		node.StatesMutex.Unlock()

		// Node Update StableCheckPoint
		node.StableCheckPoint = fStableCheckPoint
		LogStage("CHECKPOINT", true)
	}

	// print CheckPoint & MsgLogs each for Sequence
	if len(msgsLog) == len(node.NodeTable) {
		node.CheckPointHistory(msg.SequenceID)
	}
}

// Print CheckPoint History
func (node *Node) CheckPointHistory(SequenceID int64) error {
	fmt.Println("CheckPoint History!! ")

	for v, _ := range node.CheckPointMsgsLog {
		fmt.Println(" Sequence N : ", v)

		for _, j := range node.CheckPointMsgsLog[v] {
			fmt.Println("    === >", j)
		}

	}
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

	return nil
}
