package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "../utils"

var worker_logger = utils.MyLogger{utils.DEFAULT_LOG_LEVEL, "worker.go"} // default to DEBUG

type WorkerInfo struct {
	rwLock sync.RWMutex
	Id string // TODO: rename
	TaskType TaskType
	TaskId string
	InputFileLoc string
	ResultFileLoc string
	NReduce int
	// Progress float32
	State TaskState
}

var workerInfo WorkerInfo

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
			reducef func(string, []string) string) {

	// Your worker implementation here.

	// TODO: maybe use a channel for in-process comm?
	// determine task state to know which master RPC to call
	//reply := CallRegisterIdle()
	var reply *RegisterIdleReply

	//for workerInfo.State == IDLE || workerInfo.State == COMPLETED {
	for {

		if workerInfo.State == IDLE {
			reply = CallRegisterIdle()
			if reply == nil {
				worker_logger.Error("Got Error!!!!!!")
			}
		} else if workerInfo.State == COMPLETED {
			reply = CallCompletedTask() // override reply
			//if reply != nil {
			//	resetWorkerInfo()
			//	workerInfo.State = IDLE
			//}
			if reply == nil {
				worker_logger.Error("Got errror!!!!!!!!")
			}
		} else {
			worker_logger.Error("Shouldn't be in IN_PROGRESS state here...")
		}

		// TODO: maybe don't need a mutex?
		if reply.MasterCommand == ASSIGN_TASK {

			workerInfo.State = IN_PROGRESS
			workerInfo.Id = reply.WorkerId
			workerInfo.TaskType = reply.TaskType
			workerInfo.TaskId = reply.TaskId
			workerInfo.InputFileLoc = reply.InputFileLoc
			workerInfo.NReduce = reply.NReduce
			//workerInfo.Progress = 0.0

			// TODO: replace this with broadcaster/observer design
			progress_ch := make(chan float32)
			done := make(chan struct{})
			heartbeatStoped := make(chan struct {})


			// Actual computing job goroutine
			go func() {
				if workerInfo.TaskType == MAP {
					doMapTask(&workerInfo, mapf, progress_ch)
				} else if workerInfo.TaskType == REDUCE {
					doReduceTask(&workerInfo, reducef, progress_ch)
				}/* else { // None task
					close(progress_ch)
				}*/

			}()

			// Heartbeat gorountine
			go func() {
				for {
					select {
						case <-done:
							worker_logger.Debug("heartbeat job received done signal, stopping!")
							heartbeatStoped <- struct{}{}
							close(heartbeatStoped)
							return
						default:
							CallSendHeartbeat()
							time.Sleep(1*time.Second)
					}
				}

			}()


			for progress := range progress_ch {
				worker_logger.Debug(fmt.Sprintf("Task(%s) progress: %f", workerInfo.TaskId, progress))
			}
			done <- struct{}{}
			close(done)
			<- heartbeatStoped

			// Set result location & worker state
			workerInfo.State = COMPLETED

		} else if reply.MasterCommand == STAND_BY {
			worker_logger.Debug(fmt.Sprintf("Got masterCommand: %s", reply.MasterCommand))
			time.Sleep(500*time.Millisecond)
		} else if reply.MasterCommand == PLEASE_EXIT {
			worker_logger.Info(fmt.Sprintf("Got masterCommand: %s", reply.MasterCommand))
			return
		}

	}


	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func resetWorkerInfo() {
	workerInfo.TaskId = ""
	workerInfo.TaskType = NONE
	workerInfo.State = IDLE
	workerInfo.InputFileLoc = ""
	workerInfo.ResultFileLoc = ""
}

func doMapTask(info *WorkerInfo, mapf func(string, string) []KeyValue, progress_ch chan float32) {

	intermediate := make([][]KeyValue, info.NReduce, info.NReduce)
	file, err := os.Open(info.InputFileLoc)
	if err != nil {
		worker_logger.Error(fmt.Sprintf("Cannot open %v", info.InputFileLoc))
	}
	content, err := ioutil.ReadAll(file) // assume that content is always able to fit in memory
	if err != nil {
		worker_logger.Error(fmt.Sprintf("Cannot read %v", info.InputFileLoc))
	}
	file.Close()
	kva := mapf(info.InputFileLoc, string(content))

	// Distribute map result to R partitions
	for _, kv := range kva {
		bucket := ihash(kv.Key) % info.NReduce
		intermediate[bucket] = append(intermediate[bucket], kv)
	}

	// Write result as json file
	mapNum := strings.Split(info.TaskId, "-")[2]
	for i := 0; i < info.NReduce; i++ {
		oname := fmt.Sprintf("mr-%s-%d", mapNum, i)
		tmpfile, err := ioutil.TempFile("", oname)
		worker_logger.Debug(fmt.Sprintf("Writing map result to file: %v (tmp: %v)", oname, tmpfile.Name()))
		if err != nil {
			worker_logger.Error(fmt.Sprintf("Cannot create tempfile: %v", oname))
		}
		enc := json.NewEncoder(tmpfile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				worker_logger.Error(fmt.Sprintf("Intermediate file encode error"))
			}
		}
		os.Rename(tmpfile.Name(), oname)
	}
	info.ResultFileLoc = fmt.Sprintf("mr-%s-*", mapNum)
	close(progress_ch)
}

func doReduceTask(info *WorkerInfo, reducef func(string, []string) string, progress_ch chan float32) {

	filenames, err := filepath.Glob(info.InputFileLoc)
	if err != nil {
		worker_logger.Error(fmt.Sprintf("Cannot find input filepattern: %v", info.InputFileLoc))
	}

	// Assume all intermediate result can fit in memory (in memory sort)
	intermediate := []KeyValue{}
	for _, fn := range filenames {
		file, err := os.Open(fn)
		if err != nil {
			worker_logger.Error(fmt.Sprintf("Cannot open %v", fn))
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	// opening output file
	reduceNum := strings.Split(info.TaskId, "-")[2]
	oname := fmt.Sprintf("mr-out-%s", reduceNum)
	tmpfile, err := ioutil.TempFile("", oname)

	// applying reducef to every distinct key
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
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(tmpfile.Name(), oname)
	info.ResultFileLoc = oname
	close(progress_ch)
}

//
// example function to show how to make an RPC call to the master.
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
	worker_logger.Debug("Calling Master.Example...")
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallRegisterIdle() *RegisterIdleReply {

	workerInfo.State = IDLE
	worker_logger.Debug("Calling Master.RegisterIdle ...")
	args := RpcArgs{workerInfo.Id, ""}
	reply := RegisterIdleReply{}
	ok := call("Master.RegisterIdle", &args, &reply)

	if ok {
		worker_logger.Debug(fmt.Sprintf("MasterCommand: %s", reply.MasterCommand))
		worker_logger.Debug(fmt.Sprintf("Assigned worker id: %s", reply.WorkerId))
		worker_logger.Debug(fmt.Sprintf("Assigned Task id: %s", reply.TaskId))

		return &reply
	}

	log.Fatal("Can't register new idle worker!!")
	return nil
}

func CallSendHeartbeat() *HeartbeatReply {

	args := HeartbeatArgs{workerInfo.Id, workerInfo.TaskId}
	reply := HeartbeatReply{false}
	ok := call("Master.SendHeartbeat", &args, &reply)

	if ok {
		//worker_logger.Debug(fmt.Sprintf("MasterCommand: %d", reply.MasterCommand))
		worker_logger.Debug(fmt.Sprintf("Received ACK from master: %t", reply.Ack))

		return &reply
	}
	return nil
}

func CallCompletedTask() *RegisterIdleReply {

	worker_logger.Debug("Calling Master.CompletedTask ...")
	args := CompleteTaskArgs{workerInfo.Id, workerInfo.TaskId, workerInfo.ResultFileLoc}
	reply := RegisterIdleReply{}
	ok := call("Master.CompletedTask", &args, &reply)

	if ok {
		worker_logger.Debug(fmt.Sprintf("MasterCommand: %s", reply.MasterCommand))
		worker_logger.Debug(fmt.Sprintf("Assigned worker id: %s", reply.WorkerId))
		worker_logger.Debug(fmt.Sprintf("Assigned Task id: %s", reply.TaskId))

		return &reply
	}
	return nil

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
