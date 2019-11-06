// OSDI style view change.

package consensus

import(
	"fmt"
	"sync"
	"sync/atomic"
)

type VCState struct {
	NextViewID         	int64
	ViewChangeMsgLogs   *ViewChangeMsgLogs
	NewViewMsg			*NewViewMsg
	NodeID		   		string
	StableCheckPoint 	int64

	// f: the number of Byzantine faulty nodes
	// f = (n-1) / 3
	// e.g., n = 5, f = 1
	f int
}

type ViewChangeMsgLogs struct {
	// key: nodeID, value: VIEW-CHANGE message
	ViewChangeMsgs map[string]*ViewChangeMsg
	TotalViewChangeMsg  int32
	ViewChangeMsgMutex sync.RWMutex

	// Flags whether VIEW-CHANGE message has broadcasted.
	// Its value is atomically swapped by CompareAndSwapInt32.
	msgSent   int32 // atomic bool
}

func CreateViewChangeState(nodeID string, totNodes int, nextviewID int64, stablecheckpoint int64) *VCState {
	return &VCState{
		NextViewID: nextviewID,
		ViewChangeMsgLogs: &ViewChangeMsgLogs{
			ViewChangeMsgs:make(map[string]*ViewChangeMsg),
			TotalViewChangeMsg: 0,
			msgSent: 0,
		},
		NewViewMsg: nil,
		NodeID: nodeID,
		StableCheckPoint: stablecheckpoint,

		f: (totNodes - 1) / 3,
	}
}

func (vcs *VCState) ViewChange(viewchangeMsg *ViewChangeMsg) (*NewViewMsg, error) {
	// verify VIEW-CHANGE message.
	// TODO verity sender's signature
	//if err := vcs.verifyVCMsg(viewchangeMsg.NodeID, viewchangeMsg.NextViewID, viewchangeMsg.StableCheckPoint); err != nil {
	//	return nil, errors.New("view-change message is corrupted: " + err.Error() + " (nextviewID " + fmt.Sprintf("%d", viewchangeMsg.NextViewID) + ")")
	//}

	// Append VIEW-CHANGE message to its logs.
	vcs.ViewChangeMsgLogs.ViewChangeMsgMutex.Lock()
        if _, ok := vcs.ViewChangeMsgLogs.ViewChangeMsgs[viewchangeMsg.NodeID]; ok {
                fmt.Printf("View-change message from %s is already received, next view number=%d\n",
                           viewchangeMsg.NodeID, vcs.NextViewID)
		vcs.ViewChangeMsgLogs.ViewChangeMsgMutex.Unlock()
                return nil, nil
        }
	vcs.ViewChangeMsgLogs.ViewChangeMsgs[viewchangeMsg.NodeID] = viewchangeMsg
	vcs.ViewChangeMsgLogs.ViewChangeMsgMutex.Unlock()
	newTotalViewchangeMsg := atomic.AddInt32(&vcs.ViewChangeMsgLogs.TotalViewChangeMsg, 1)

	// Print current voting status.
	fmt.Printf("[View-Change-Vote]: %d\n", newTotalViewchangeMsg)

	// Return NEW-VIEW message only once.
	if int(newTotalViewchangeMsg) >= 2*vcs.f + 1 &&
	   atomic.CompareAndSwapInt32(&vcs.ViewChangeMsgLogs.msgSent, 0, 1) {
		return &NewViewMsg{
			NextViewID: vcs.NextViewID,
			NodeID: vcs.NodeID,
			SetViewChangeMsgs: vcs.GetViewChangeMsgs(),
			SetPrePrepareMsgs: nil,
			Max_S: 0,
			Min_S: 0,
		}, nil
	}

	return nil, nil
}

func (vcs *VCState) GetViewChangeMsgs() map[string]*ViewChangeMsg {
	newMap := make(map[string]*ViewChangeMsg)

	vcs.ViewChangeMsgLogs.ViewChangeMsgMutex.RLock()
	for k, v := range vcs.ViewChangeMsgLogs.ViewChangeMsgs {
		newMap[k] = v
	}
	vcs.ViewChangeMsgLogs.ViewChangeMsgMutex.RUnlock()

	return newMap
}

func (vcs *VCState) verifyVCMsg(nodeID string, nextViewID int64, stableCheckPoint int64) error {
	// Wrong view. That is, wrong configurations of peers to start the consensus.
	// TODO vertify sender's signature

	return nil
}

func (state *State) ClearMsgLogs() {
	// intialize anything of MsgLogs but request and reply
	state.MsgLogs.PrePrepareMsg = nil
	for seq, _ := range state.MsgLogs.PrepareMsgs {
		state.MsgLogs.PrepareMsgs[seq] = nil
	}
	for seq, _ := range state.MsgLogs.CommitMsgs {
		state.MsgLogs.CommitMsgs[seq] = nil
	}
	state.MsgLogs.TotalPrepareMsg = 0
	state.MsgLogs.TotalCommitMsg = 0
	state.MsgLogs.commitMsgSent = 0
}

func (state *State) Redo_SetState(viewID int64, nodeID string, totNodes int, preprepareMsg *PrePrepareMsg, digest string) *State {
	state.ViewID = viewID
	state.NodeID = nodeID
	state.MsgLogs.PrePrepareMsg = preprepareMsg
	state.MsgLogs.TotalPrepareMsg = 0
	state.MsgLogs.TotalCommitMsg = 0
	state.MsgLogs.commitMsgSent = 0
	state.MsgLogs.replyMsgSent = 0

	state.F = (totNodes - 1) / 3
	state.succChkPointDelete = 0
	state.digest = digest

	return state
}