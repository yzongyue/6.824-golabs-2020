package mr

import (
	"log"
	"../utils"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var logger = utils.MyLogger{utils.DEBUG} // default to DEBUG

type Master struct {
	// Your definitions here.

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
	//ret := true
	logger.Debug("in done!")


	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// TODO: figure out better place to setup log flags
	log.SetFlags(log.Ltime | log.Lshortfile)


	m := Master{}

	// Your code here.


	m.server()
	return &m
}
