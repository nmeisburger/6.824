package raft

import "log"

// Debug ...
const Debug = 0

// DPrintf debug printf
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type levelType uint8

const (
	// INFO first logging level
	INFO levelType = 1
	// DEBUG1 second logging level
	DEBUG1 levelType = 2
	// DEBUG2 third logging level
	DEBUG2 levelType = 3
	// DEBUG3 fourth logging level
	DEBUG3 levelType = 4
)

const logLevel levelType = 0

// LPrintf is a leveled printf
func LPrintf(level levelType, format string, a ...interface{}) (n int, err error) {
	if logLevel > level {
		log.Printf(format, a...)
	}
	return
}
