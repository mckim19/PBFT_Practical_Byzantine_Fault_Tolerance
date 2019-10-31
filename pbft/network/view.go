package network

import (
	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
	"fmt"

	"sync/atomic"
	"unsafe"
)

func (node *Node) StartViewChange() {
	var vcs *consensus.VCState

	// Start_ViewChange
	LogStage("ViewChange", false)

	// Create nextviewid.
	var nextviewid = node.View.ID + 1
	vcs = node.VCState
	for vcs == nil {
		vcs = consensus.CreateViewChangeState(node.MyInfo.NodeID, len(node.NodeTable), nextviewid, node.StableCheckPoint)
		// Assign new VCState if node did not create the state.
		if !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&node.VCState)), unsafe.Pointer(nil), unsafe.Pointer(vcs)) {
			vcs = node.VCState
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

func (node *Node) GetViewChange(viewchangeMsg *consensus.ViewChangeMsg) {
	var vcs *consensus.VCState

	LogMsg(viewchangeMsg)

	// Ignore VIEW-CHANGE message if the next view id is not new.
	var nextviewid = node.View.ID + 1
	if nextviewid > viewchangeMsg.NextViewID {
		return
	}
	nextviewid = viewchangeMsg.NextViewID

	vcs = node.VCState
	for vcs == nil {
		// Ignore VIEW-CHANGE message if the next view id is not new.
		nextviewid = node.View.ID + 1
		if nextviewid > viewchangeMsg.NextViewID {
			return
		}
		nextviewid = viewchangeMsg.NextViewID

		// Create a view state for the next view id.
		vcs = consensus.CreateViewChangeState(node.MyInfo.NodeID, len(node.NodeTable), nextviewid, node.StableCheckPoint)
		// Assign new VCState if node did not create the state.
		if !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&node.VCState)), unsafe.Pointer(nil), unsafe.Pointer(vcs)) {
			vcs = node.VCState
		}
	}

	newViewMsg, err := vcs.ViewChange(viewchangeMsg)
	if err != nil {
		fmt.Println(err)
		return
	}

	// From OSDI: When the primary of view v + 1 receives 2f valid
	// view-change messages for view v + 1 from other replicas,
	// it multicasts a NEW-VIEW message to all other replicas.
	var nextPrimary = node.getPrimaryInfoByID(nextviewid)
	if newViewMsg == nil || node.MyInfo != nextPrimary {
		return
	}

	// Change View and Primary.
	node.updateView(newViewMsg.NextViewID)

	// Fill all the fields of NEW-VIEW message.
	node.fillNewViewMsg(newViewMsg)

	for i := int64(1); i < int64(len(newViewMsg.SetPrePrepareMsgs)); i++ {
		fmt.Println("************************************************************************")
		fmt.Println(newViewMsg.SetPrePrepareMsgs[i])
	}
	LogStage("NewView", false)

	LogMsg(newViewMsg)

	node.Broadcast(newViewMsg, "/newview")
	LogStage("NewView", true)

	node.VCState = nil

	node.IsViewChanging = false
	// TODO this node has to start redo
}

func (node *Node) fillNewViewMsg(newViewMsg *consensus.NewViewMsg) {
	// Search min_s the sequence number of the latest stable checkpoint and
	// max_s the highest sequence number in a prepare message in V.
	var min_s int64 = 0
	var max_s int64 = 0

	fmt.Println("***********************N E W V I E W***************************")
	for _, vcm := range newViewMsg.SetViewChangeMsgs {
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

	for _, vcm := range newViewMsg.SetViewChangeMsgs {
		for seq, setpm := range vcm.SetP {
			if newMap[seq] == nil {
				digest := setpm.PrePrepareMsg.Digest
				newMap[seq] = GetPrePrepareForNewview(newViewMsg.NextViewID, seq, digest)
			}
		}
	}
	newViewMsg.SetPrePrepareMsgs = newMap
}

func (node *Node) GetNewView(msg *consensus.NewViewMsg) error{

	fmt.Printf("<<<<<<<<<<<<<<<<NewView>>>>>>>>>>>>>>>>: %d by %s\n", msg.NextViewID, msg.NodeID)

	// TODO this node has to start redo

	//Change View and Primary
	node.updateView(msg.NextViewID)

	node.VCState = nil

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
