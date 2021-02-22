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

// Add your RPC definitions here.
type RpcArgs struct {
	WorkerId string
	ResultLoc string
}

type CompleteTaskArgs struct {
	WorkerId string
	TaskId string
	ResultLoc string
}

type RegisterIdleReply struct {
	MasterCommand MasterCommand
	TaskType TaskType
	TaskId string
	InputFileLoc string
	WorkerId string // master will assign id to registered idle workers
	NReduce int
}

//type CompletedTaskReply struct {
//	MasterCommand MasterCommand
//	TaskType TaskType
//	InputFileLoc string
//}

type HeartbeatArgs struct {
	WorkerId string
	TaskId string
}

type HeartbeatReply struct {
	Ack bool
}

type MasterCommand int
const (
	ASSIGN_TASK MasterCommand = iota
	STAND_BY
	PLEASE_EXIT
)
func (cmd MasterCommand) String() string {
	switch cmd {
	case ASSIGN_TASK:
		return "ASSIGN_TASK"
	case STAND_BY:
		return "STAND_BY"
	case PLEASE_EXIT:
		return "PLEASE_EXIT"
	default:
		return "-"
	}
}

type TaskType int
const (
	MAP TaskType = iota
	REDUCE
	NONE
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
