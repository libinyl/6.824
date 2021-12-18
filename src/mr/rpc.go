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

const (
	TaskStatusIdle       = "idle"
	TaskStatusInProgress = "in-progress"
	TaskStatusCompleted  = "completed"
)

type WorkerInitHandShakeReq struct {
}

type WorkerInitHandShakeRsp struct {
	ReduceCnt  int
	AssignedId int
}

type RequestTaskReq struct {
	WorkerId int
}

type RequestTaskRsp struct {
	TaskType        string
	MapTaskId       int
	MapTaskFilename string
	ReduceTaskId    int
}

// Add your RPC definitions here.
type RegisterWorkerReq struct {
}

type RegisterWorkerRsp struct {
	WorkerId int
}

type ReportStatusReq struct {
	WorkerId   int
	TaskType   string
	TaskId     int
	TaskStatus string
}

type ReportStatusRsp struct {
	NReduce   int
	MapTaskId int
	File      string
	Func      string
}

//type HeartBeatReq struct {
//	workerId int
//	TaskStatus str
//}
//
//type HeartBeatRsp struct {
//}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
