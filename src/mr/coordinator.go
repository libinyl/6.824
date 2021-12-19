package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
)

const (
	workerStatusLive = "live"
	workerStatusDie  = "die"
)

type Coordinator struct {
	// Your definitions here.

	workerStatus sync.Map
	mapTasks     []*mapTask
	reduceTasks  []*reduceTask

	completedMapTasksCnt    int
	completedReduceTasksCnt int

	newWorkerId func() int
}

type mapTask struct {
	id   int
	file string

	mu     sync.Mutex
	status string
}

type reduceTask struct {
	id int

	mu     sync.Mutex
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
	rsp.ReduceCnt = len(c.reduceTasks)
	rsp.AssignedId = c.newWorkerId()

	// master 更新该worker的状态
	c.workerStatus.Store(rsp.AssignedId, workerStatusLive)
	log.Printf("接收到 worker 注册请求.已为其分配 id:%v", rsp.AssignedId)
	return nil
}

// RequestTask worker 向 master 请求分配任务
// master 找到一个 idle 的 task,分配给该 worker
func (c *Coordinator) RequestTask(req *RequestTaskReq, rsp *RequestTaskRsp) error {
	log.Printf("接收到 worker [%v] 的分配请求.", req.WorkerId)

	// master 更新该worker的状态
	c.workerStatus.Store(req.WorkerId, workerStatusLive)

	isAllMapDone := func() bool {
		log.Printf("当前已完成 maptask 数量:%v/%v", c.completedMapTasksCnt, len(c.mapTasks))
		log.Printf("当前已完成 reduce 数量:%v/%v", c.completedReduceTasksCnt, len(c.reduceTasks))

		return c.completedMapTasksCnt == len(c.mapTasks)
	}

	assigned := false

	if !isAllMapDone() {
		// 找到一个 idle 的 task
		for _, task := range c.mapTasks {
			task.mu.Lock()
			if task.status == TaskStatusIdle {
				rsp.TaskType = "map"
				rsp.MapTaskId = task.id
				rsp.MapTaskFilename = task.file
				task.status = TaskStatusInProgress
				assigned = true
				log.Printf("已为 worker [%v] 分配了 map 任务[%v]", req.WorkerId, task.id)
			}
			task.mu.Unlock()
			if assigned {
				return nil
			}
		}
		log.Printf("未找到适合分配的 map task")
		return nil
	}

	// 如果所有 map 都执行完了, 就从 reduceTask 里找可分配的任务

	for _, task := range c.reduceTasks {
		task.mu.Lock()
		if task.status == TaskStatusIdle {
			rsp.TaskType = "reduce"
			rsp.ReduceTaskId = task.id
			task.status = TaskStatusInProgress
			assigned = true
			log.Printf("已为 worker [%v] 分配了 reduce 任务[%v]", req.WorkerId, task.id)
		}
		task.mu.Unlock()
		if assigned {
			return nil
		}
	}
	log.Printf("未找到适合分配的 reduce task")
	return nil
}

// ReportStatus 上报 worker 状态和任务信息
func (c *Coordinator) ReportStatus(req *ReportStatusReq, rsp *ReportStatusRsp) error {
	log.Printf("接收到 worker[%v]的汇报,[%v]任务[%v]状态为[%v]", req.WorkerId, req.TaskType, req.TaskId, req.TaskStatus)

	// 首先标注该 worker 是正常的
	c.workerStatus.Store(req.WorkerId, workerStatusLive)

	// 更新任务状态
	updated := false
	if req.TaskType == "map" {
		for _, task := range c.mapTasks {
			task.mu.Lock()
			if task.id == req.TaskId {
				task.status = req.TaskStatus
				if task.status == TaskStatusCompleted {
					c.completedMapTasksCnt++
				}
				updated = true
			}
			task.mu.Unlock()
			if updated {
				return nil
			}
		}

		return nil
	}

	if req.TaskType == "reduce" {
		for _, task := range c.reduceTasks {
			task.mu.Lock()
			if task.id == req.TaskId {
				task.status = req.TaskStatus
				if task.status == TaskStatusCompleted {
					c.completedReduceTasksCnt++
				}
				updated = true
			}
			task.mu.Unlock()
			if updated {
				return nil
			}
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
	//mu, e := net.Listen("tcp", ":1234")
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
	//ret := false

	// Your code here.
	//ret = c.completedReduceTasksCnt == len(c.reduceTasks)
	done := c.completedReduceTasksCnt == len(c.reduceTasks)
	if done {
		log.Printf("所有任务执行完毕,程序即将退出")
	}
	return done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	c := Coordinator{}

	// Your code here.
	c.newWorkerId = newIdGenerator()

	// 初始化maptask
	for i, file := range files {
		c.mapTasks = append(c.mapTasks, &mapTask{
			id:     i,
			file:   file,
			status: TaskStatusIdle,
		})
	}
	// 初始化 reducetask
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, &reduceTask{
			id:     i,
			status: TaskStatusIdle,
		})
	}

	log.Printf("server started. state: %+v", c)

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
