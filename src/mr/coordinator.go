package mr

import (
	"log"
	"sync"
	"sync/atomic"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	workerStatusLive = "live"
	workerStatusDie  = "die"
)

type Coordinator struct {
	// Your definitions here.

	nReduce      int
	workerStatus sync.Map
	mapTasks     []*mapTask
	reduceTasks  []*reduceTask

	completedMapTasksCnt    int
	completedReduceTasksCnt int

	newWorkerId func() int
}

type mapTask struct {
	l sync.RWMutex

	taskId int
	file   string
	status string
}

type reduceTask struct {
	l sync.RWMutex

	taskId int
	status string
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// WorkerInitHandShake worker 启动时与服务握手
func (c *Coordinator) WorkerInitHandShake(req *WorkerInitHandShakeReq, rsp *WorkerInitHandShakeRsp) error {
	// worker 拿到 reduce任务数,得知自己的 id
	rsp.ReduceCnt = c.nReduce
	rsp.AssignedId = c.newWorkerId()

	// master 更新该worker的状态
	c.workerStatus.Store(rsp.AssignedId, workerStatusLive)
	return nil
}

// RequestTask worker 向 master 请求分配任务
// master 找到一个 idle 的 task,分配给该 worker
func (c *Coordinator) RequestTask(req *RequestTaskReq, rsp *RequestTaskRsp) error {
	// master 更新该worker的状态
	c.workerStatus.Store(req.WorkerId, workerStatusLive)

	isAllMapDone := func() bool {
		return int(c.completedMapTasksCnt) == len(c.mapTasks)
	}

	if !isAllMapDone() {
		// 找到一个 idle 的 task
		for _, task := range c.mapTasks {
			task.l.RLock()
			if task.status == TaskStatusIdle {
				rsp.MapTaskFilename = task.file
				task.status = TaskStatusInProgress
			}
			task.l.RUnlock()
		}
		return nil
	}

	// 如果所有 map 都执行完了, 就从 reduceTask 里找可分配的任务
	for _, task := range c.reduceTasks {
		task.l.RLock()
		if task.status == TaskStatusIdle {
			rsp.ReduceTaskId = task.taskId
			task.status = TaskStatusInProgress
		}
		task.l.RUnlock()
	}

	return nil
}

// ReportStatus 上报 worker 状态和任务信息
func (c *Coordinator) ReportStatus(req *ReportStatusReq, rsp *ReportStatusRsp) error {
	// 首先标注该 worker 是正常的
	c.workerStatus.Store(req.WorkerId, workerStatusLive)

	// 更新任务状态
	if req.TaskType == "map" {
		for _, task := range c.mapTasks {
			task.l.RLock()
			if task.taskId == req.TaskId {
				task.status = req.TaskStatus
				if task.status == TaskStatusCompleted {
					c.completedMapTasksCnt++
				}
			}
			task.l.RUnlock()
		}
	}

	if req.TaskType == "reduce" {
		for _, task := range c.reduceTasks {
			task.l.RLock()
			if task.taskId == req.TaskId {
				task.status = req.TaskStatus
				if task.status == TaskStatusCompleted {
					c.completedReduceTasksCnt++
				}
			}
			task.l.RLock()
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.completedReduceTasksCnt == len(c.reduceTasks)

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.newWorkerId = newIdGenerator()

	// 初始化maptask
	for i, file := range files {
		c.mapTasks = append(c.mapTasks, &mapTask{
			taskId: i,
			file:   file,
			status: TaskStatusIdle,
		})
	}
	// 初始化 reducetask
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, &reduceTask{
			taskId: i,
			status: TaskStatusIdle,
		})
	}

	c.server()
	return &c
}

var newIdGenerator = func() func() int {
	id := int32(-1)
	return func() int {
		atomic.AddInt32(&id, 1)
		return int(id)
	}
}
