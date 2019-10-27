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
	NodeID		   string
	StableCheckPoint int64

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



func CreateViewChangeState(nodeID string, totNodes int, nextviewID int64, stablecheckpoint int64) *ViewChangeState {
	return &ViewChangeState{
		NextViewID: nextviewID,
		ViewChangeMsgLogs: &ViewChangeMsgLogs{
			ViewChangeMsgs:make(map[string]*ViewChangeMsg),

			TotalViewChangeMsg: 0,
		},
		NodeID : nodeID,
		StableCheckPoint: stablecheckpoint,

		f: (totNodes - 1) / 3,
	}
}


func (viewchangestate *ViewChangeState) CreateViewChangeMsg(setp map[int64]*SetPm, setc map[string]*CheckPointMsg) (*ViewChangeMsg, error) {

	return &ViewChangeMsg{
		NodeID: viewchangestate.NodeID,
		NextViewID: viewchangestate.NextViewID,
		StableCheckPoint: viewchangestate.StableCheckPoint,
		SetC: setc,
		SetP: setp,
	}, nil

	return nil, nil
}


func (viewchangestate *ViewChangeState) ViewChange(viewchangeMsg *ViewChangeMsg) (*NewViewMsg, error) {

	//TODO: verify viewchangeMsg

	//check received viewchangeMsg SetP
	/*
	for v, _ := range viewchangeMsg.SetP {
		fmt.Println("    === > Preprepare : ", viewchangeMsg.SetP[v].PrePrepareMsg)
		fmt.Println("    === > Prepare : ", viewchangeMsg.SetP[v].PrepareMsgs)
	}

	fmt.Println("**************a set of SetC******************")
	for c, _ := range viewchangeMsg.SetC {
		fmt.Println("    === > checkpoint : ", viewchangeMsg.SetC[c])
	}
	*/
	// Append msg to its logs
	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgMutex.Lock()
	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgs[viewchangeMsg.NodeID] = viewchangeMsg
	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgMutex.Unlock()
	newTotalViewchangeMsg := atomic.AddInt32(&viewchangestate.ViewChangeMsgLogs.TotalViewChangeMsg,1)

	// Print current voting status
	fmt.Printf("[<<<<<<<<ViewChange-Vote>>>>>>>>>>]: %d\n", newTotalViewchangeMsg)


	if int(newTotalViewchangeMsg) >= 2*viewchangestate.f+1 && viewchangestate.viewchanged() {
			
		//setviewchangemsgs := make(map[string]*ViewChangeMsg)

		//setviewchangemsgs = viewchangestate.GetViewChangeMsgs()

		return &NewViewMsg{
			NextViewID: viewchangestate.NextViewID,
			NodeID: viewchangestate.NodeID,
			SetViewChangeMsgs: viewchangestate.GetViewChangeMsgs(),
		}, nil
		
	}

	return nil, nil
}


func (viewchangestate *ViewChangeState) viewchanged() bool {
	if int(atomic.LoadInt32(&viewchangestate.ViewChangeMsgLogs.TotalViewChangeMsg)) < 2*viewchangestate.f +1{
		return false
	}

	return true
}

func (viewchangestate *ViewChangeState) GetViewChangeMsgs() map[string]*ViewChangeMsg {
	newMap := make(map[string]*ViewChangeMsg)

	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgMutex.Lock()
	for k, v := range viewchangestate.ViewChangeMsgLogs.ViewChangeMsgs {
		newMap[k] = v
	}
	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgMutex.Unlock()

	return newMap
}

