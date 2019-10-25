package network

import (
	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
	"fmt"
)

func (node *Node) StartViewChange() {
	//Start_ViewChange
	LogStage("ViewChange", false) //ViewChange_Start

	//stop accepting Msgs  
	close(node.MsgEntrance)
	fmt.Println("close Entrance")
	//Create nextviewid
	var nextviewid =  node.View.ID + 1

	//Create ViewChangeState
	node.ViewChangeState = consensus.CreateViewChangeState(node.MyInfo.NodeID, len(node.NodeTable), nextviewid, node.StableCheckPoint)
	fmt.Println("CreateViewChangeState")
	//a set of PreprepareMsg and PrepareMsgs for veiwchange
	setp := make(map[int64]*consensus.SetPm)

	node.StatesMutex.RLock()
	for seqID, state := range node.States {
		var setPm consensus.SetPm
		setPm.PrePrepareMsg = state.GetPrePrepareMsg()
		setPm.PrepareMsgs = state.GetPrepareMsgs()
		setp[seqID] = &setPm
	}
	node.StatesMutex.RUnlock()

	fmt.Println("Create Setp")
	//Create ViewChangeMsg
	viewChangeMsg, err := node.ViewChangeState.CreateViewChangeMsg(setp)
	fmt.Println("CreateViewChangeMsg")
	if err != nil {
		node.MsgError <- []error{err}
		return
	}

	node.Broadcast(viewChangeMsg, "/viewchange")
	fmt.Println("Breadcast viewchange")
}

func (node *Node) NewView(newviewMsg *consensus.NewViewMsg) error {
	LogMsg(newviewMsg)

	node.Broadcast(newviewMsg, "/newview")
	LogStage("NewView", true)

	return nil
}

func (node *Node) GetViewChange(viewchangeMsg *consensus.ViewChangeMsg) error {
	LogMsg(viewchangeMsg)

	if node.ViewChangeState == nil {
		return nil
	}

	//newViewMsg, err := node.ViewChangeState.ViewChange(viewchangeMsg)
	newView, err := node.ViewChangeState.ViewChange(viewchangeMsg)
	if err != nil {
		return err
	}

	LogStage("ViewChange", true)

	if newView != nil && node.isMyNodePrimary() {
		//Change View and Primary
		node.updateView(newView.NextViewID)

		fmt.Println("newView")

		LogStage("NewView", false)
		node.NewView(newView)

	}

	return nil
}

func (node *Node) GetNewView(msg *consensus.NewViewMsg) error {

	//Change View and Primary
	node.updateView(msg.NextViewID)

	fmt.Printf("<<<<<<<<NewView>>>>>>>>: %d by %s\n", msg.NextViewID, msg.NodeID)
	return nil
}

func (node *Node) updateView(viewID int64) {
	node.View.ID = viewID
	viewIdx := viewID % int64(len(node.NodeTable))
	node.View.Primary = node.NodeTable[viewIdx]

	fmt.Println("ViewID:", node.View.ID, "Primary:", node.View.Primary.NodeID)
}

func (node *Node) isMyNodePrimary() bool {
	return node.MyInfo.NodeID == node.View.Primary.NodeID
}
