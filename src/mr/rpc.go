package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType int

const (
	TaskMap TaskType = iota
	TaskReduce
	TaskWait
	TaskExit
)

// Workers ask coordinator for tasks
type TaskArgs struct {
}

type TaskReply struct {
	TaskType TaskType
	TaskID   int
	Attempt  int
	Filename string
	NMap     int
	NReduce  int
}

// Workers tell coordinator when they finished
type FinishTaskArgs struct {
	TaskType TaskType
	TaskID   int
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
