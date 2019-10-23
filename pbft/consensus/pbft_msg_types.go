package consensus

// Messages are SOSP style.

type RequestMsg struct {
	Timestamp  int64  `json:"timestamp"`
	ClientID   string `json:"clientID"`
	Operation  string `json:"operation"`
	Data       string `json:"data"`
	SequenceID int64  `json:"sequenceID"`
}

type ReplyMsg struct {
	ViewID    int64  `json:"viewID"`
	Timestamp int64  `json:"timestamp"`
	ClientID  string `json:"clientID"`
	NodeID    string `json:"nodeID"`
	Result    string `json:"result"`
}

type PrePrepareMsg struct {
	ViewID     int64       `json:"viewID"`
	SequenceID int64       `json:"sequenceID"`
	Digest     string      `json:"digest"`
	RequestMsg *RequestMsg `json:"requestMsg"`
}

type VoteMsg struct {
	ViewID     int64  `json:"viewID"`
	SequenceID int64  `json:"sequenceID"`
	Digest     string `json:"digest"`
	NodeID     string `json:"nodeID"`
	MsgType    `json:"msgType"`
}
type CheckPointMsg struct {
	SequenceID int64  `json:"sequenceID"`
	Digest     string `json:"digest"`
	NodeID     string `json:"nodeID"`
}

type ViewChangeMsg struct {
	NodeID     string `json:"nodeID"`
	NextViewID int64  `json:"nextviewID"`
	StableCheckPoint int64 `json:"stableCheckPoint`
	//C checkpointmsg_set 2f+1
	//P a set of preprepare + (preparemsg * 2f+1) from stablecheckpoint to the biggest sequence_num that node received

}

type NewViewMsg struct {
	NodeID     string `json:"nodeID"`
	NextViewID int64  `json:"nextviewID"`
	//V a set containing the valid ViewChageMsg
	//O a set of PrePrepareMsgs from latest stable checkpoint(min-s) in V to the highest sequence number(max-s) in a PrepareMsg in V
	//  new Primary creates a new PrePrepareMsg for view v+1 for each sequence number between min-s and max-s
}

type MsgType int

const (
	PrepareMsg MsgType = iota
	CommitMsg
)
