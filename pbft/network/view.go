package network

import (
	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
	"fmt"
)

func (node *Node) StartViewChange() {
	//Start_ViewChange
	LogStage("ViewChange", false) //ViewChange_Start

	//stop accepting Msgs  
	//close(node.MsgEntrance)
	//fmt.Println("close Entrance")
	//Create nextviewid
	var nextviewid = node.View.ID + 1

	//Create ViewChangeState
	if node.ViewChangeState == nil {
		node.ViewChangeState = consensus.CreateViewChangeState(node.MyInfo.NodeID, len(node.NodeTable), nextviewid, node.StableCheckPoint)
	}

	//a set of PreprepareMsg and PrepareMsgs for veiwchange
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
	fmt.Println("node.CheckPointMsgsLog[node.StableCheckPoint]", node.CheckPointMsgsLog[node.StableCheckPoint])
	setc = node.CheckPointMsgsLog[node.StableCheckPoint]
	fmt.Println("setc",setc)


	node.StatesMutex.RUnlock()

	//Create ViewChangeMsg
	viewChangeMsg, err := node.ViewChangeState.CreateViewChangeMsg(setp, setc)

	if err != nil {
		node.MsgError <- []error{err}
		return
	}

	node.Broadcast(viewChangeMsg, "/viewchange")
	fmt.Println("Breadcast viewchange")
	LogStage("ViewChange", true)

	//node.GetViewChange(viewChangeMsg) ???????
}

func (node *Node) NewView(newviewMsg *consensus.NewViewMsg) error {
	LogMsg(newviewMsg)

	node.Broadcast(newviewMsg, "/newview")
	LogStage("NewView", true)

	node.ViewChangeState = nil
	
	node.IsViewChanging = false
	return nil
}

func (node *Node) GetViewChange(viewchangeMsg *consensus.ViewChangeMsg) error {
	LogMsg(viewchangeMsg)

	//Create nextviewid
	var nextviewid =  node.View.ID + 1

	if node.ViewChangeState == nil && node.View.ID != viewchangeMsg.NextViewID{

		//Create ViewChangeState
		node.ViewChangeState = consensus.CreateViewChangeState(node.MyInfo.NodeID, len(node.NodeTable), nextviewid, node.StableCheckPoint)
	}

	newView, err := node.ViewChangeState.ViewChange(viewchangeMsg)
	if err != nil {
		return err
	}

	var nextPrimary = node.getPrimaryInfoByID(nextviewid)

	if newView != nil && node.MyInfo == nextPrimary {
		//Change View and Primary
		node.updateView(newView.NextViewID)


		//Search min_s the sequence number of the latest stable checkpoint and
		//max_s the highest sequence number in a prepare message in V
		var min_s int64 
		min_s = 0
		var max_s int64
		max_s = 0


		fmt.Println("**************N E W V I E W******************")
		for nv, _ := range newView.SetViewChangeMsgs {
			fmt.Println("    === > newView.SetViewChangeMsgs : ", newView.SetViewChangeMsgs[nv])
			fmt.Println("    === > newView.SetViewChangeMsgs..StableCheckPoint : ", newView.SetViewChangeMsgs[nv].StableCheckPoint)
			
			if min_s < newView.SetViewChangeMsgs[nv].StableCheckPoint {
				min_s = newView.SetViewChangeMsgs[nv].StableCheckPoint 
			}

			for seq, _ := range newView.SetViewChangeMsgs[nv].SetP {
				fmt.Println("seq ", seq)
				fmt.Println("newView.SetViewChangeMsgs.SetP : ", newView.SetViewChangeMsgs[nv].SetP[seq])
				for nodeid, _ := range newView.SetViewChangeMsgs[nv].SetP[seq].PrepareMsgs {
					if max_s < newView.SetViewChangeMsgs[nv].SetP[seq].PrepareMsgs[nodeid].SequenceID {
						max_s = newView.SetViewChangeMsgs[nv].SetP[seq].PrepareMsgs[nodeid].SequenceID
					}
				}
				//if newView.SetViewChangeMsgs[nv].SetP[seq].PrePrepareMsg //is there valid prepreparemsg + 
				//if len(newView.SetViewChangeMsgs[nv].SetP[seq].PrepareMsgs) >= valid 2f prepare || newView.SetPrePrepareMsgs[seq] == nil
				//	newView.SetPrePrepareMsgs[seq] =  PrePrepareMsg(newView.NextViewID, seq , node.States[seq].requestdigest)
				//else 
				//	newView.SetPrePrepareMsgs[seq] =  nil	
			}
		}

		//if newView.SetPrePrepareMsgs[seq] ==  nil
		//	for seq, _ := range newView.SetViewChangeMsgs[nv].SetP
		//		newView.SetPrePrepareMsgs[seq] =  PrePrepareMsg(newView.NextViewID, seq , nil)

		fmt.Println("min_s ", min_s, "max_s", max_s)
		fmt.Println("newView")

		LogStage("NewView", false)
		node.NewView(newView)

	}

	return nil
}

func (node *Node) GetNewView(msg *consensus.NewViewMsg) error {

	fmt.Printf("<<<<<<<<NewView>>>>>>>>: %d by %s\n", msg.NextViewID, msg.NodeID)

	//Change View and Primary
	node.updateView(msg.NextViewID)

	node.ViewChangeState = nil

	node.IsViewChanging = false
	return nil
}

func (node *Node) updateView(viewID int64) {
	node.View.ID = viewID
	node.View.Primary = node.getPrimaryInfoByID(viewID)

	fmt.Println("ViewID:", node.View.ID, "Primary:", node.View.Primary.NodeID)
}

func (node *Node) isMyNodePrimary() bool {
	return node.MyInfo.NodeID == node.View.Primary.NodeID
}

func (node *Node) getPrimaryInfoByID(viewID int64) *NodeInfo {
	viewIdx := viewID % int64(len(node.NodeTable))
	return node.NodeTable[viewIdx]
}
