package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// ExampleArgs is the example rpc input
type ExampleArgs struct {
	X int
}

// ExampleReply is the example rpc input
type ExampleReply struct {
	Y int
}

// JobRequest is sent to get a new job
type JobRequest struct{}

// MapJob represents the info for a map job
type MapJob struct {
	NoneRemaining bool
	Filename      string
	MapID         int
	NReduce       int
}

// ReduceJob represents the info for a reduce job
type ReduceJob struct {
	NoneRemaining bool
	ReduceID      int
	NMap          int
}

// MapComplete is sent to indicate that a map job has completed
type MapComplete struct {
	ID int
}

// ReduceComplete is sent to indicate that a reduce job has completed
type ReduceComplete struct {
	ID int
}

// CompleteAck acknowledges receipt of a complete message
type CompleteAck struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
