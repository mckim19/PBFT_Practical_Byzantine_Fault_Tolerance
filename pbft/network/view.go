package network

import (
	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
	"fmt"

	"sync/atomic"
	"unsafe"
)

func (node *Node) StartViewChange() {
	var vcs *consensus.ViewChangeState

	// Start_ViewChange
	LogStage("ViewChange", false)

	// Create nextviewid.
	var nextviewid = node.View.ID + 1
	vcs = node.ViewChangeState
	for vcs == nil {
		vcs = consensus.CreateViewChangeState(node.MyInfo.NodeID, len(node.NodeTable), nextviewid, node.StableCheckPoint)
		// Assign new ViewChangeState if node did not create the state.
		if !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&node.ViewChangeState)), unsafe.Pointer(nil), unsafe.Pointer(vcs)) {
			vcs = node.ViewChangeState
		}
	}

	// a set of PreprepareMsg and PrepareMsgs for veiwchange.
	setp := make(map[int64]*consensus.SetPm)
	setc := make(map[string]*consensus.CheckPointMsg)
	
	node.StatesMutex.RLock()
	for seqID, state := range node.States {
		var setPm consensus.SetPm
		setPm.PrePrepareMsg = state.GetPrePrepareMsg()
		setPm.PrepareMsgs = state.GetPrepareMsgs()
		setp[seqID] = &setPm
	}
	
	fmt.Println("node.StableCheckPoint : ", node.StableCheckPoint)
	setc = node.CheckPointMsgsLog[node.StableCheckPoint]
	fmt.Println("setc",setc)


	node.StatesMutex.RUnlock()

	// Create ViewChangeMsg.
	viewChangeMsg, err := vcs.CreateViewChangeMsg(setp, setc)

	if err != nil {
		node.MsgError <- []error{err}
		return
	}

	node.Broadcast(viewChangeMsg, "/viewchange")
	fmt.Println("Breadcast viewchange")
	LogStage("ViewChange", true)

	node.GetViewChange(viewChangeMsg)
}

func (node *Node) NewView(newviewMsg *consensus.NewViewMsg) {
	LogMsg(newviewMsg)

	node.Broadcast(newviewMsg, "/newview")
	LogStage("NewView", true)

	node.ViewChangeState = nil
	
	node.IsViewChanging = false
	// TODO this node has to start redo
}

func (node *Node) GetViewChange(viewchangeMsg *consensus.ViewChangeMsg) {
	var vcs *consensus.ViewChangeState

	LogMsg(viewchangeMsg)

	// Ignore VIEW-CHANGE message if the next view id is not new.
	var nextviewid = node.View.ID + 1
	if nextviewid > viewchangeMsg.NextViewID {
		return
	}
	nextviewid = viewchangeMsg.NextViewID

	vcs = node.ViewChangeState
	for vcs == nil {
		// Ignore VIEW-CHANGE message if the next view id is not new.
		nextviewid = node.View.ID + 1
		if nextviewid > viewchangeMsg.NextViewID {
			return
		}
		nextviewid = viewchangeMsg.NextViewID

		// Create a view state for the next view id.
		vcs = consensus.CreateViewChangeState(node.MyInfo.NodeID, len(node.NodeTable), nextviewid, node.StableCheckPoint)
		// Assign new ViewChangeState if node did not create the state.
		if !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&node.ViewChangeState)), unsafe.Pointer(nil), unsafe.Pointer(vcs)) {
			vcs = node.ViewChangeState
		}
	}

	newView, err := vcs.ViewChange(viewchangeMsg)
	if err != nil {
		fmt.Println(err)
		return
	}

	// From OSDI: When the primary of view v + 1 receives 2f valid
	// view-change messages for view v + 1 from other replicas,
	// it multicasts a NEW-VIEW message to all other replicas.
	var nextPrimary = node.getPrimaryInfoByID(nextviewid)
	if newView == nil || node.MyInfo != nextPrimary {
		return
	}

	// Change View and Primary.
	node.updateView(newView.NextViewID)

	// Search min_s the sequence number of the latest stable checkpoint and
	// max_s the highest sequence number in a prepare message in V.
	var min_s int64 = 0
	var max_s int64 = 0

	fmt.Println("***********************N E W V I E W***************************")
	for _, vcm := range newView.SetViewChangeMsgs {
		if min_s < vcm.StableCheckPoint {
			min_s = vcm.StableCheckPoint
		}

		for seq, prepareSet := range vcm.SetP {
			if seq < max_s {
				continue
			}
			for _, prepareMsg := range prepareSet.PrepareMsgs {
				if max_s < prepareMsg.SequenceID {
					max_s = prepareMsg.SequenceID
				}
			}
		}
	}

	fmt.Println("min_s ", min_s, "max_s", max_s)

	// Create SetPrePrepareMsgs of the new-view for redo
	// only if a preprepare message of the SetPrePrepareMsgs with sequence number seq is nil.
	newMap := make(map[int64]*consensus.PrePrepareMsg)

	for _, vcm := range newView.SetViewChangeMsgs {
		for seq, setpm := range vcm.SetP {
			if newMap[seq] == nil {
				digest := setpm.PrePrepareMsg.Digest
				newMap[seq] = GetPrePrepareForNewview(newView.NextViewID, seq, digest)
			}
		}
	}
	newView.SetPrePrepareMsgs = newMap

	for i := int64(1); i < int64(len(newView.SetPrePrepareMsgs)); i++ {
		fmt.Println("************************************************************************")
		fmt.Println(newView.SetPrePrepareMsgs[i])
	}
	LogStage("NewView", false)
	node.NewView(newView)
}

func (node *Node) GetNewView(msg *consensus.NewViewMsg) error{

	fmt.Printf("<<<<<<<<<<<<<<<<NewView>>>>>>>>>>>>>>>>: %d by %s\n", msg.NextViewID, msg.NodeID)

	// TODO this node has to start redo

	//Change View and Primary
	node.updateView(msg.NextViewID)

	node.ViewChangeState = nil

	node.IsViewChanging = false

	return nil
}

func (node *Node) updateView(viewID int64) {
	node.View.ID = viewID
	node.View.Primary = node.getPrimaryInfoByID(viewID)
}

func (node *Node) isMyNodePrimary() bool {
	return node.MyInfo.NodeID == node.View.Primary.NodeID
}

func (node *Node) getPrimaryInfoByID(viewID int64) *NodeInfo {
	viewIdx := viewID % int64(len(node.NodeTable))
	return node.NodeTable[viewIdx]
}

func GetPrePrepareForNewview(nextviewID int64, sequenceid int64, digest string) *consensus.PrePrepareMsg {
	return &consensus.PrePrepareMsg{
		ViewID:     nextviewID,
		SequenceID: sequenceid,
		Digest:     digest,
	}
}
