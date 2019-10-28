package network

import (
	"fmt"
	"time"
	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
)

func LogMsg(msg interface{}) {
	t := time.Now().UnixNano()

	switch m := msg.(type) {
	case *consensus.RequestMsg:
		fmt.Printf("%d: [REQUEST] ClientID: %s, Timestamp: %d, Operation: %s\n", t, m.ClientID, m.Timestamp, m.Operation)
	case *consensus.PrePrepareMsg:
		fmt.Printf("%d: [PREPREPARE] SequenceID: %d\n", t, m.SequenceID)
	case *consensus.VoteMsg:
		if m.MsgType == consensus.PrepareMsg {
			fmt.Printf("%d: [PREPARE] NodeID: %s\n", t, m.NodeID)
		} else if m.MsgType == consensus.CommitMsg {
			fmt.Printf("%d: [COMMIT] NodeID: %s\n", t, m.NodeID)
		}
	case *consensus.ReplyMsg:
		fmt.Printf("%d: [REPLY] Result: %s by %s\n", t, m.Result, m.NodeID)
	case *consensus.CheckPointMsg:
		fmt.Printf("%d: [CheckPointMsg] NodeID: %s\n", t, m.NodeID)
	case *consensus.ViewChangeMsg:
		fmt.Printf("%d: [ViewChangeMsg] NodeID: %s\n", t, m.NodeID)
	}
}

func LogStage(stage string, isDone bool) {
	if isDone {
		fmt.Printf("[STAGE-DONE] %s\n", stage)
	} else {
		fmt.Printf("[STAGE-BEGIN] %s\n", stage)
	}
}
