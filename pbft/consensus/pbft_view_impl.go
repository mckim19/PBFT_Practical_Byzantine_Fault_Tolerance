// OSDI style view change.

package consensus

import(
	"fmt"
	"sync"
	"sync/atomic"
)

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
	// key: nodeID, value: VIEW-CHANGE message
	ViewChangeMsgs map[string]*ViewChangeMsg
	TotalViewChangeMsg  int32
	ViewChangeMsgMutex sync.RWMutex

	// Flags whether VIEW-CHANGE message has broadcasted.
	// Its value is atomically swapped by CompareAndSwapInt32.
	msgSent   int32 // atomic bool
}

func CreateViewChangeState(nodeID string, totNodes int, nextviewID int64, stablecheckpoint int64) *ViewChangeState {
	return &ViewChangeState{
		NextViewID: nextviewID,
		ViewChangeMsgLogs: &ViewChangeMsgLogs{
			ViewChangeMsgs:make(map[string]*ViewChangeMsg),
			TotalViewChangeMsg: 0,
			msgSent: 0,
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
	// TODO: verify VIEW-CHANGE message.

	// Append VIEW-CHANGE message to its logs.
	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgMutex.Lock()
        if _, ok := viewchangestate.ViewChangeMsgLogs.ViewChangeMsgs[viewchangeMsg.NodeID]; ok {
                fmt.Printf("View-change message from %s is already received, next view number=%d\n",
                           viewchangeMsg.NodeID, viewchangestate.NextViewID)
		viewchangestate.ViewChangeMsgLogs.ViewChangeMsgMutex.Unlock()
                return nil, nil
        }
	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgs[viewchangeMsg.NodeID] = viewchangeMsg
	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgMutex.Unlock()
	newTotalViewchangeMsg := atomic.AddInt32(&viewchangestate.ViewChangeMsgLogs.TotalViewChangeMsg, 1)

	// Print current voting status.
	fmt.Printf("[View-Change-Vote]: %d\n", newTotalViewchangeMsg)

	// Return NEW-VIEW message only once.
	if int(newTotalViewchangeMsg) >= 2*viewchangestate.f + 1 &&
	   atomic.CompareAndSwapInt32(&viewchangestate.ViewChangeMsgLogs.msgSent, 0, 1) {
		return &NewViewMsg{
			NextViewID: viewchangestate.NextViewID,
			NodeID: viewchangestate.NodeID,
			SetViewChangeMsgs: viewchangestate.GetViewChangeMsgs(),
			SetPrePrepareMsgs: nil,
		}, nil
	}

	return nil, nil
}

func (viewchangestate *ViewChangeState) GetViewChangeMsgs() map[string]*ViewChangeMsg {
	newMap := make(map[string]*ViewChangeMsg)

	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgMutex.RLock()
	for k, v := range viewchangestate.ViewChangeMsgLogs.ViewChangeMsgs {
		newMap[k] = v
	}
	viewchangestate.ViewChangeMsgLogs.ViewChangeMsgMutex.RUnlock()

	return newMap
}
