package mr

import (
	"fmt"
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
			//workerInfo.Progress = 0.0

			progress_ch := make(chan float32)
			done := make(chan struct{})
			heartbeatStoped := make(chan struct {})
			// TODO: replace this with broadcaster/observer design

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
			workerInfo.ResultFileLoc = fmt.Sprintf("dummy-%s.txt", strings.Split(workerInfo.TaskId, "-")[2])

		} else if reply.MasterCommand == STAND_BY {
			worker_logger.Debug(fmt.Sprintf("Got masterCommand: %s", reply.MasterCommand))
			time.Sleep(100*time.Millisecond)
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

func doMapTask(info *WorkerInfo, mapf func(string, string) []KeyValue, ch chan float32) {

	// TODO: replace this for loop with actually map job
	N := 2
	for i := 0; i < N; i++ {
		time.Sleep(1*time.Second)
		ch <- float32(i+1)/float32(N)
	}
	close(ch)
}

func doReduceTask(info *WorkerInfo, reducef func(string, []string) string, ch chan float32) {

	// TODO: replace this for loop with actually map job
	N := 2
	for i := 0; i < N; i++ {
		time.Sleep(1*time.Second)
		ch <- float32(i+1)/float32(N)
	}
	close(ch)
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
