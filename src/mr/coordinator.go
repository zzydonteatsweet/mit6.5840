package mr

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

var registerLock sync.Mutex

//JobPhase->0:Map 1:Reduce
type Coordinator struct {
	// Your definitions here.
	JobQueue   chan *Job
	JobPhase   int
	WorkerSize int
}

type Job struct {
	JobType    int
	InputFiles []string
	StartTime  time.Time
}

func (c *Coordinator) RegisterWorker(tmp *int, id *int) error {
	fmt.Println("register worker")
	registerLock.Lock()
	defer registerLock.Unlock()
	c.WorkerSize++
	*id = c.WorkerSize
	return nil
}

func (c *Coordinator) DistributeJob(workerId *int, job *Job) error {
	fmt.Printf("DistributeJob id %v \n", workerId)
	job = <-c.JobQueue
	return nil
}

func (c *Coordinator) FinishJob(workerId *int, jobId *int) error {
	fmt.Println("server job finished", jobId)
	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
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
	if c.WorkerSize > 0 && len(c.JobQueue) == 0 {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("making coordinator...")
	c := Coordinator{
		JobQueue: make(chan *Job, nReduce),
		JobPhase: 0,
	}
	// Your code here.
	for _, file := range files {
		fmt.Println("insert file ", file)
		tmp := new(Job)
		tmp.JobType = 0
		tmp.InputFiles = []string{file}
		tmp.StartTime = time.Now()
		c.JobQueue <- tmp
		fmt.Printf("file %v inserted\n", file)
	}
	c.server()
	return &c
}
