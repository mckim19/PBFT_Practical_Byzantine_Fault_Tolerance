package consensus

import(
	"fmt"
	"sync"
	"sync/atomic"
)

const one = 1

type ViewChangeState struct {
	NextViewID         int64
	ViewChangeMsgLogs   *ViewChangeMsgLogs
	CurrentStage   ViewChangeStage
	NodeID		   string
	StableCheckPoint int64
	//SetP   []*SetPm
	// f: the number of Byzantine faulty nodes
	// f = (n-1) / 3
	// e.g., n = 5, f = 1
	f int
}


type ViewChangeMsgLogs struct {
	ViewChangeMsgs map[string]*ViewChangeMsg

	TotalViewChangeMsg  int32

	ViewChangeMsgMutex sync.Mutex
}



type ViewChangeStage int
const (
	ViewIdle        ViewChangeStage = iota // Node is created successfully, but the consensus process is not started yet.
	ViewChanged
	NewViewed
)


func CreateViewChangeState(nodeID string, totNodes int, nextviewID int64, stablecheckpoint int64, /*setpm SetP*/) *ViewChangeState {
	return &ViewChangeState{
		NextViewID: nextviewID,
		ViewChangeMsgLogs: &ViewChangeMsgLogs{
			ViewChangeMsgs:make(map[string]*ViewChangeMsg),

			TotalViewChangeMsg: 0,
		},
		CurrentStage: ViewIdle,
		NodeID : nodeID,
		StableCheckPoint: stablecheckpoint,
		//SetP: setpm,

		f: (totNodes - 1) / 3,
	}
}


func (viewchangestate *ViewChangeState) CreateViewChangeMsg(/*states *States*/) (*ViewChangeMsg, error) {

	viewchangestate.CurrentStage = ViewChanged

	/*
	var setp []*SetPm

	for i := 0; i < len(states); i++ {
		setp[i].PreprepareMsg = states[i].MsgLogs.PreprepareMsgs
		setp[i].PrepareMsgs
	}

	for v, _ := range states {
		fmt.Println(" Sequence N : ", v)
		fmt.Println("    === > Preprepare : ", states[v].MsgLogs.PreprepareMsgs)
		fmt.Println("    === > Prepare : ", states[v].MsgLogs.PrepareMsgs)
	}*/

	return &ViewChangeMsg{
		NodeID: viewchangestate.NodeID,
		NextViewID: viewchangestate.NextViewID,
		StableCheckPoint: viewchangestate.StableCheckPoint,

		//SetP: viewchangestes.SetP,
	}, nil



	return nil, nil
}


func (viewchangestate *ViewChangeState) ViewChange(viewchangeMsg *ViewChangeMsg) (*NewViewMsg, error) {

	//TODO: verify viewchangeMsg


	// Append msg to its logs
	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgMutex.Lock()
	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgs[viewchangeMsg.NodeID] = viewchangeMsg
	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgMutex.Unlock()
	newTotalViewchangeMsg := atomic.AddInt32(&viewchangestate.ViewChangeMsgLogs.TotalViewChangeMsg,1)

	// Print current voting status
	fmt.Printf("[<<<<<<<<ViewChange-Vote>>>>>>>>>>]: %d\n", newTotalViewchangeMsg)


	if int(newTotalViewchangeMsg) > 2*viewchangestate.f {
		return nil, nil
	}

	if viewchangestate.viewchanged() {

		viewchangestate.CurrentStage = NewViewed

		
		return &NewViewMsg{
			NextViewID: viewchangestate.NextViewID,
			NodeID : viewchangestate.NodeID,
		}, nil
		
	}

	return nil, nil
}


func (viewchangestate *ViewChangeState) viewchanged() bool {
	if len(viewchangestate.ViewChangeMsgLogs.ViewChangeMsgs) < 2*viewchangestate.f {
		return false
	}

	return true
}
