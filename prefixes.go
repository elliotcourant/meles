package meles

type metaPrefix = byte

const (
	metaPrefix_MyId                metaPrefix = '@'
	metaPrefix_Log                            = 'l'
	metaPrefix_StableStore                    = 's'
	metaPrefix_IncrementSequence              = '+'
	metaPrefix_DistributedSequence            = '#'
)
