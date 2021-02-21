package mr

import (
	"../utils"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"github.com/jedib0t/go-pretty/table"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var logger = utils.MyLogger{utils.DEFAULT_LOG_LEVEL, "master.go"} // default to DEBUG

type Master struct {
	// Your definitions here.
	TaskSummary sync.Map // each call request from a worker is a goroutine that coudl change this field
	mu sync.Mutex
	workerCount int // TODO: wrap in struct SharedInt
	taskCount int
}

type KVPair struct {
	K string
	V TaskInfo
}

type TaskInfo struct {
	TaskId string
	CurrentState TaskState
	WorkerId string
	InputLoc string
	ResultLoc string
	SinceLastHeartbeat int
}

type TaskState int
const (
	IDLE TaskState = iota
	IN_PROGRESS
	COMPLETED
)

func (s TaskState) String() string {
	switch s {
	case IDLE:
		return "IDLE"
	case IN_PROGRESS:
		return "RUNNING"
	case COMPLETED:
		return "COMPLETE"
	default:
		return "-"
	}
}


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
	//return errors.New("test error 1")
}

func (m *Master) RegisterIdle(args *RpcArgs, reply *RegisterIdleReply) error {

	// Register new worker
	if args.WorkerId == "" {
		// TODO: keep track of in-progress/idle/fail-recovering worker counts
		var newWorkerId string
		m.mu.Lock()
		m.workerCount += 1
		newWorkerId = fmt.Sprintf("worker-%d", m.workerCount)
		m.mu.Unlock()
		reply.WorkerId = newWorkerId
		m.assignTask(newWorkerId, reply)
	} else { // Existing worker
		reply.WorkerId = args.WorkerId
		m.assignTask(args.WorkerId, reply)
	}

	return nil
}

func (m *Master) SendHeartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {

	// set TaskInfo.SinceLastHeartbeat = 0
	logger.Debug(fmt.Sprintf("Got heartbeat from worker: %s (TaskId: %s)", args.WorkerId, args.TaskId))
	value, ok := m.TaskSummary.Load(args.TaskId)
	if ok {
		info, ok := value.(TaskInfo)
		if ok {
			if info.WorkerId != args.WorkerId {
				log.Fatal("WorkerId won't match!!")
			}
			info.CurrentState = IN_PROGRESS
			info.SinceLastHeartbeat = 0
			m.TaskSummary.Store(args.TaskId, info)
			reply.Ack = true
		}
	}

	return nil
}

func (m *Master) CompletedTask(args *CompleteTaskArgs, reply *RegisterIdleReply) error {

	// Update task status and result location
	value, ok := m.TaskSummary.Load(args.TaskId)
	if ok {
		info, ok := value.(TaskInfo)
		if ok {
			if info.WorkerId != args.WorkerId {
				log.Fatal("WorkerId won't match!!")
			}
			info.CurrentState = COMPLETED
			info.SinceLastHeartbeat = -1
			info.ResultLoc = args.ResultLoc
			m.TaskSummary.Store(args.TaskId, info)

			// get & reply with next highest priority idle job (if no more task then send PLEASE-EXIT reply)
			m.assignTask(args.WorkerId, reply)
		}
	}

	return nil
}


func (m *Master) printTaskSummary() string {

	t := table.NewWriter()
	t.AppendHeader(table.Row{"id", "state", "worker", "input", "output", "since"})

	var copy []KVPair
	//m.TaskSummary.Range(func(key, value interface{}) bool {
	m.TaskSummary.Range(func(key, value interface{}) bool {

		//copy[fmt.Sprint(key)] = value
		info, ok := value.(TaskInfo)
		if ok {
			copy = append(copy, KVPair{fmt.Sprint(key), info})
			//t.AppendRow([]interface{}{fmt.Sprint(key), info.CurrentState, info.WorkerId, info.InputLoc, info.ResultLoc, info.SinceLastHeartbeat})
		}
		return true
	})

	sort.Slice(copy, func(i, j int) bool {return copy[i].K < copy[j].K })

	for _, kv := range copy {
		t.AppendRow([]interface{}{kv.V.TaskId, kv.V.CurrentState, kv.V.WorkerId, kv.V.InputLoc, kv.V.ResultLoc, kv.V.SinceLastHeartbeat})
	}

	return t.Render()
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	// show taskSummary
	logger.Debug(fmt.Sprintf("TaskSummary:\n%s", m.printTaskSummary()))

	// Increment SinceLastHeartbeat field for each in-progress tasks
	m.increaseSinceLastHeartbeat()

	// Stop and reschedule task for task with SinceLastHeartbeat > 10
	// TODO: Implement fail recovery
	/*deadWorkerId, deadTaskId := m.checkDeadWorker()
	if deadTaskId != "" {
		// fail recovery
		logger.Debug("Fail recovery for deadWOrker() and deadTask() ...")
		m.resetTaskToIdle(deadTaskId)
	}*/

	// Check if there is any undo task left
	if m.checkAllTaskFinished() {
		ret = true
	}

	return ret
}

func (m *Master) increaseSinceLastHeartbeat() {

	m.TaskSummary.Range(func(key, value interface{}) bool {
		info, ok := value.(TaskInfo)
		if ok {
			if info.CurrentState == IN_PROGRESS {
				info.SinceLastHeartbeat += 1
				m.TaskSummary.Store(key, info)
			}
		}
		return true
	})
}

func (m *Master) findNextTask(t TaskType, st TaskState) (*TaskInfo, bool) {

	var retTaskInfo *TaskInfo

	hasIdleTask := false

	m.TaskSummary.Range(func(key, value interface{}) bool {
		//copy[fmt.Sprint(key)] = value
		curTaskId := fmt.Sprint(key)
		info, ok := value.(TaskInfo)
		if ok {
			if info.CurrentState == st {
				if (t == MAP && strings.Contains(curTaskId, "map")) ||
				   (t == REDUCE && strings.Contains(curTaskId, "reduce")) {

					hasIdleTask = true
					retTaskInfo = &info
					return false // stop iteration on first matched task
				}
				return true
			}
			return true
		}
		return false
	})

	if hasIdleTask {
		return retTaskInfo, hasIdleTask
	} else {
		return nil, false
	}

}

func (m *Master) assignTask(workerId string, reply *RegisterIdleReply) bool {

	hasIdleMapTask := false

	// Find next idle map task:  O(M+R)
	// Check if all map tasks are completed; Assign any idle map task to idle worker
	nextMapTaskInfo, hasIdleMapTask := m.findNextTask(MAP, IDLE)
	if hasIdleMapTask {

		nextMapTaskInfo.WorkerId = workerId
		nextMapTaskInfo.CurrentState = IN_PROGRESS
		nextMapTaskInfo.SinceLastHeartbeat = 0
		m.TaskSummary.Store(nextMapTaskInfo.TaskId, *nextMapTaskInfo)

		// Update reply
		reply.WorkerId = workerId
		reply.MasterCommand = ASSIGN_TASK
		reply.TaskType = MAP
		reply.TaskId = nextMapTaskInfo.TaskId
		reply.InputFileLoc = nextMapTaskInfo.InputLoc

		return true

	} else {

		_, hasRunningMapTask := m.findNextTask(MAP, IN_PROGRESS)
		if hasRunningMapTask {
			// Update reply
			reply.WorkerId = workerId
			reply.MasterCommand = STAND_BY
			return true
		} else {
			nextReduceTaskInfo, hasIdleReduceTask := m.findNextTask(REDUCE, IDLE)

			if hasIdleReduceTask {
				// Update picked taskInfo
				nextReduceTaskInfo.WorkerId = workerId
				nextReduceTaskInfo.CurrentState = IN_PROGRESS
				nextReduceTaskInfo.SinceLastHeartbeat = 0
				m.TaskSummary.Store(nextReduceTaskInfo.TaskId, *nextReduceTaskInfo)

				// Update reply
				reply.WorkerId = workerId
				reply.MasterCommand = ASSIGN_TASK
				reply.TaskType = REDUCE
				reply.TaskId = nextReduceTaskInfo.TaskId
				reply.InputFileLoc = "idk???" // TODO: figure out how to know intermediate result location

				return true
			} else {
				_, hasRunningReduceTask := m.findNextTask(REDUCE, IN_PROGRESS)
				if hasRunningReduceTask {
					// Update reply
					reply.WorkerId = workerId
					reply.MasterCommand = STAND_BY
					return true
				} else {
					// exit worker since no more task left
					reply.WorkerId = workerId
					reply.MasterCommand = PLEASE_EXIT

					// unregsiter worker
					// TODO: keep track of in-progress/idle/fail-recovering worker counts
					m.mu.Lock()
					m.workerCount -= 1
					m.mu.Unlock()
				}
			}
		}
		return false

	}

}

func (m *Master) checkAllTaskFinished() bool {

	hasUnfinishedTask := false

	m.TaskSummary.Range(func(key, value interface{}) bool {
		info, ok := value.(TaskInfo)
		if ok {
			if info.CurrentState != COMPLETED {
				hasUnfinishedTask = true
				return false
			}
			return true
		}
		return false
	})

	return !hasUnfinishedTask
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// TODO: figure out better place to setup log flags
	log.SetFlags(log.Ltime) // | log.Lshortfile)

	m := Master{}

	// Your code here.
	// Generating Map tasks
	logger.Debug("Generating Map tasks...")
	for i, fn := range files { // M map tasks
		m.taskCount++
		taskId := fmt.Sprintf("map-task-%d", i)
		taskInfo := TaskInfo{taskId, IDLE, "", fn, "", -1}
		m.TaskSummary.Store(taskId, taskInfo)
	}
	// Generating Reduce tasks
	logger.Debug("Generating Reduce tasks...")
	for i := 0; i < nReduce; i++ { // R reduce tasks
		m.taskCount++
		taskId := fmt.Sprintf("reduce-task-%d", i)
		taskInfo := TaskInfo{taskId,IDLE, "", "", "", -1}
		m.TaskSummary.Store(taskId, taskInfo)
	}

	m.server()
	return &m
}
