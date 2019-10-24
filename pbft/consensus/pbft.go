package consensus

type PBFT interface {
	StartConsensus(request *RequestMsg, sequenceID int64) (*PrePrepareMsg, error)
	PrePrepare(prePrepareMsg *PrePrepareMsg) (*VoteMsg, error)
	Prepare(prepareMsg *VoteMsg) (*VoteMsg, error)
	Commit(commitMsg *VoteMsg) (*ReplyMsg, *RequestMsg, error)
	//ViewChange(viewchangeMsg *ViewChangeMsg) (error , error)
}
