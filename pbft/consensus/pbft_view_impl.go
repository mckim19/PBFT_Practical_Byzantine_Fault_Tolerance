package consensus

import(
	"fmt"
	"sync"
	"sync/atomic"
)

const one = 1

type ViewChangeState struct {
	ViewID         int64
	ViewChangeMsgLogs   *ViewChangeMsgLogs
	CurrentStage   ViewChangeStage
	NodeID		   string

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


func CreateViewChangeState(nodeID string, totNodes int, viewID int64) *ViewChangeState {
	return &ViewChangeState{
		ViewID: viewID,
		ViewChangeMsgLogs: &ViewChangeMsgLogs{
			ViewChangeMsgs:make(map[string]*ViewChangeMsg),

			TotalViewChangeMsg: 0,
		},
		CurrentStage: ViewIdle,
		NodeID : nodeID,

		f: (totNodes - 1) / 3,
	}
}


func StartViewChange(nodeID string) {


} 


func (viewchangestate *ViewChangeState) CreateViewChangeMsg() (*ViewChangeMsg, error) {

	viewchangestate.CurrentStage = ViewChanged


	return &ViewChangeMsg{
		NextViewID: ((viewchangestate.ViewID + 1) % int64(3*viewchangestate.f +1)),
		NodeID: viewchangestate.NodeID,
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
	fmt.Printf("[ViewChange-Vote]: %d\n", newTotalViewchangeMsg)


	if int(newTotalViewchangeMsg) > 2*viewchangestate.f {
		return nil, nil
	}

	if viewchangestate.viewchanged() {

		viewchangestate.CurrentStage = NewViewed

		
		return &NewViewMsg{
			NextViewID: viewchangestate.ViewID,
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
