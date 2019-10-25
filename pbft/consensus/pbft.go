package consensus

type PBFT interface {
	StartConsensus(request *RequestMsg, sequenceID int64) (*PrePrepareMsg, error)
	PrePrepare(prePrepareMsg *PrePrepareMsg) (*VoteMsg, error)
	Prepare(prepareMsg *VoteMsg) (*VoteMsg, error)
	Commit(commitMsg *VoteMsg) (*ReplyMsg, *RequestMsg, error)

	GetSequenceID() int64
	GetF() int

	GetMsgReceiveChannel() <-chan interface{}
	GetMsgSendChannel() chan<- interface{}

	GetReqMsg() *RequestMsg
	GetPrePrepareMsg() *PrePrepareMsg
	GetPrepareMsgs() map[string]*VoteMsg
	GetCommitMsgs() map[string]*VoteMsg
}
