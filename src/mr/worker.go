package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func taskFilename(mapTaskId, reduceTaskId int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskId, reduceTaskId)
}

func saveMapOutput(mapTaskId int, mapOutput []KeyValue) error {
	log.Printf("saveMapOutput: taskid:[%v],kv lenth: [%v]", mapTaskId, len(mapOutput))
	sort.Sort(ByKey(mapOutput))

	indexKv := func() map[string][]KeyValue {
		file2KVs := make(map[string][]KeyValue)
		for _, kv := range mapOutput {
			reduceTaskId := ihash(kv.Key) % nReduce
			filename := taskFilename(mapTaskId, reduceTaskId)
			file2KVs[filename] = append(file2KVs[filename], kv)
		}
		return file2KVs
	}

	file2KVs := indexKv()

	for file, kvs := range file2KVs {
		file, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
		if err != nil {
			return err
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		for _, kv := range kvs {
			err := encoder.Encode(&kv)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

var (
	mu sync.Mutex

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

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 1. 请求 master 分配 workerid
	myId, nReduce = initHandShake()

	// 2. 每秒钟汇报一次状态
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			mu.Lock()
			reportStatus(myId, currTaskId, currTaskType, currTaskStatus)
			mu.Unlock()
		}
	}()

	// 3. 每秒请求一次任务,如果有就执行
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			taskType, mapTaskId, mapFilename, reduceTaskId := requestTask(myId)
			switch taskType {
			case "":
				log.Printf("worker:[%v],暂无可执行的任务", myId)
			case "map":
				log.Printf("已得到分配的 map 任务.TaskId:[%v], filename:[%v]", mapTaskId, mapFilename)
				mu.Lock()
				currTaskType = "map"
				currTaskStatus = TaskStatusInProgress
				currTaskId = mapTaskId
				handleMap(mapTaskId, mapFilename, mapf)
				currTaskStatus = TaskStatusCompleted
				mu.Unlock()
			case "reduce":
				mu.Lock()
				log.Printf("已得到分配的 reduce 任务.TaskId:[%v]", reduceTaskId)

				currTaskType = "reduce"
				currTaskStatus = TaskStatusInProgress
				currTaskId = reduceTaskId
				handleReduce(reduceTaskId, reducef)
				currTaskStatus = TaskStatusCompleted
				mu.Unlock()
			default:
				log.Fatal("未能识别的taskType:%v", taskType)
			}
		}
	}()
	select {}
}

// handleMap 处理 map 任务
func handleMap(taskId int, filename string, mapf func(string, string) []KeyValue) {
	content := string(readFile(filename))
	err := saveMapOutput(taskId, mapf(filename, content))
	if err != nil {
		log.Fatalf("saveMapOutput err:%v", err)
	}
}

// handleReduce 处理 reduce 任务
func handleReduce(taskId int, reducef func(string, []string) string) {
	collectTargetFiles := func(reduceTaskId int) []string {
		targetfiles := []string{}
		fileInfos, err := ioutil.ReadDir(".")
		if err != nil {
			log.Fatal(err)
		}
		for _, fi := range fileInfos {
			filename := fi.Name()
			parts := strings.Split(filename, "-")
			if len(parts) == 3 {
				Y, err := strconv.Atoi(parts[2])
				if err != nil {
					continue
				}
				if Y == taskId {
					targetfiles = append(targetfiles, filename)
				}
			}
		}
		return targetfiles
	}
	targetFiles := collectTargetFiles(taskId)

	var allKvs []KeyValue
	for _, f := range targetFiles {
		fileStream, err2 := os.Open(f)
		if err2 != nil {
			log.Fatal(err2)
		}
		dec := json.NewDecoder(fileStream)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			allKvs = append(allKvs, kv)
		}
	}

	sort.Sort(ByKey(allKvs))

	saveReduce(fmt.Sprintf("mr-out-%d", taskId), allKvs, reducef)
}

func saveReduce(oname string, intermediate []KeyValue, reducef func(string, []string) string) {
	log.Printf("saveReduce to [%v],intermediate size:%v", oname, len(intermediate))

	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatal(err)
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
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

func readFile(filename string) []byte {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}
