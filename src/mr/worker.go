package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func saveMapOutput(mapTaskId, nReduce int, mapKey string, mapOutput []KeyValue) error {
	reduceTaskId := ihash(mapKey) % nReduce
	filename := fmt.Sprintf("mr-%d-%d", mapTaskId, reduceTaskId)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	for _, kv := range mapOutput {
		err := encoder.Encode(&kv)
		if err != nil {
			return err
		}
	}
	return nil
}

var (
	myId    int
	nReduce int

	currTaskType   string
	currTaskId     int
	currTaskStatus string
)

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// 1. 请求 master 分配 workerid
	myId, nReduce = initHandShake()

	// 2. 每秒钟汇报一次状态
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			reportStatus(myId, currTaskId, currTaskType, currTaskStatus)
		}
	}()

	// 3. 每秒请求一次任务,如果有就执行
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			taskType, mapTaskId, mapFilename, reduceTaskId := requestTask(myId)
			if taskType == "map" {
				handleMap(mapTaskId, mapFilename)
			} else {
				handleReduce(reduceTaskId)
			}
		}
	}()
	select {}
}

// 接收到任务, 干活

func handleMap(taskId int, filename string) {

}

func handleReduce(taskId int) {

}

func reportStatus(workerId, taskId int, taskType, taskStatus string) (int, int, string, string) {
	req := &ReportStatusReq{
		WorkerId:   workerId,
		TaskType:   taskType,
		TaskId:     taskId,
		TaskStatus: taskStatus,
	}
	rsp := &ReportStatusRsp{}
	call("Coordinator.ReportStatus", req, rsp)
	return rsp.NReduce, rsp.MapTaskId, rsp.File, rsp.Func
}

func initHandShake() (int, int) {
	req := &WorkerInitHandShakeReq{}
	rsp := &WorkerInitHandShakeRsp{}
	call("Coordinator.WorkerInitHandShake", req, rsp)
	return rsp.AssignedId, rsp.ReduceCnt
}

func requestTask(workerId int) (taskType string, mapTaskId int, mapTaskFilename string, reduceTaskId int) {
	req := &RequestTaskReq{WorkerId: workerId}
	rsp := &RequestTaskRsp{}
	call("Coordinator.RequestTask", req, rsp)
	return rsp.TaskType, rsp.MapTaskId, rsp.MapTaskFilename, rsp.ReduceTaskId
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func readFile(filename string) string {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}
