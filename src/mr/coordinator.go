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

var distributeLock sync.Mutex

type Coordinator struct {
	// Your definitions here.
	JobQueue chan *Job
	// 0 Map 1 Reduce 2 Finished
	JobPhase      int
	MapCnt        int
	ReduceCnt     int
	MapJobList    []*Job
	ReduceJobList []*Job
}

type Job struct {
	JobId int
	//  0 Not Started 1 Doing 2 Finish
	JobProgress int
	// 0 Map 1 Reduce 2 Finished
	JobType    int
	InputFiles []string
	StartTime  time.Time
	MapId      int
	ReduceCnt  int
	MapCnt     int
}

func (c *Coordinator) DistributeJob(workerId *int, job *Job) error {
	distributeLock.Lock()
	fmt.Printf("DistributeJob workerId %v \n", *workerId)
	if c.JobPhase == 2 {
		doneJob := Job{}
		doneJob.JobType = 2
		*job = doneJob
		return nil
	}
	*job = *<-c.JobQueue
	job.JobProgress = 1
	job.StartTime = time.Now()
	distributeLock.Unlock()
	fmt.Printf("DistributeJob %v \n", *job)
	return nil
}

func (c *Coordinator) FinishJob(workerId *int, jobId *int) error {
	fmt.Println("server job finished", jobId)
	if *jobId < c.MapCnt {
		c.MapJobList[*jobId].JobProgress = 2
		c.insertReduceTask(*jobId)
	} else {
		c.ReduceJobList[*jobId-c.MapCnt].JobProgress = 2
		c.updateJobPhaseFinishedForDone()
	}
	return nil
}

func (c *Coordinator) checkReduceTaskDone() {
	ok := true
	for _, job := range c.ReduceJobList {
		if job.JobProgress < 2 {
			ok = false
			break
		}
	}
	if ok {
		c.JobPhase = 2
	}
}

func (c *Coordinator) insertReduceTask(jobId int) {
	c.ReduceJobList[jobId].ReduceCnt = c.ReduceCnt
	c.ReduceJobList[jobId].JobType = 1
	c.ReduceJobList[jobId].JobId = jobId + c.MapCnt
	c.ReduceJobList[jobId].JobProgress = 0
	c.ReduceJobList[jobId].MapId = jobId
	c.ReduceJobList[jobId].MapCnt = c.MapCnt

	for i := 0; i < c.ReduceCnt; i++ {
		filename := fmt.Sprintf("mr-tmp-%d-%d.txt", jobId, i)
		c.ReduceJobList[jobId].InputFiles = append(c.ReduceJobList[jobId].InputFiles, filename)
	}
	c.JobQueue <- c.ReduceJobList[jobId]
	c.updateJobPhaseFinishedForReduce()
}

func (c *Coordinator) updateJobPhaseFinishedForReduce() {
	ok := true
	for _, job := range c.MapJobList {
		if job.JobProgress == 1 {
			ok = false
			break
		}
	}
	if ok {
		c.JobPhase = 1
		time.Sleep(500)
	}
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
	return c.JobPhase == 2
}

func (c *Coordinator) checkMapJobStatusNormal() bool {
	ret := true
	currentTime := time.Now()
	for i := 0; i < len(c.MapJobList); i++ {
		if c.MapJobList[i].JobType == 1 && currentTime.Sub(c.MapJobList[i].StartTime) > 10*time.Minute {
			c.MapJobList[i].JobType = 0
			c.JobQueue <- c.MapJobList[i]
		} else {
			ret = false
		}
	}
	return ret
}

func (c *Coordinator) checkReduceJobStatusNormal() bool {
	ret := true
	currentTime := time.Now()
	for i := 0; i < c.ReduceCnt; i++ {
		ret = ret && (c.ReduceJobList[i].JobType == 1 && currentTime.Sub(c.ReduceJobList[i].StartTime) > 10*time.Minute)
	}
	return ret
}

func (c *Coordinator) checkMapPhaseFinished() bool {
	ret := true
	for i := 0; i < len(c.MapJobList); i++ {
		ret = ret && (c.MapJobList[i].JobType == 2)
	}
	if ret {
		go c.checkReduceJobStatusNormal()
	}
	return ret
}

func (c *Coordinator) JobIsDone(jobId *int, state *int) error {
	c.MapJobList[*jobId].JobType++
	*state = 1
	return nil
}

func (c *Coordinator) updateJobPhaseFinishedForDone() {
	ok := true
	for i := 0; i < c.ReduceCnt; i++ {
		if c.ReduceJobList[i].JobProgress == 1 {
			ok = false
			break
		}
	}
	if ok {
		c.JobPhase = 2
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("making coordinator...")
	c := Coordinator{
		JobQueue:      make(chan *Job, nReduce),
		JobPhase:      0,
		MapCnt:        len(files),
		ReduceCnt:     nReduce,
		MapJobList:    make([]*Job, 0),
		ReduceJobList: make([]*Job, 0),
	}
	// Your code here.
	for i, file := range files {
		fmt.Println("insert file ", file)
		tmp := new(Job)
		tmp.JobType = 0
		tmp.InputFiles = []string{}
		tmp.InputFiles = append(tmp.InputFiles, file)
		tmp.ReduceCnt = nReduce
		tmp.JobId = i
		tmp.MapId = i
		tmp.MapCnt = len(files)
		c.MapJobList = append(c.MapJobList, tmp)
		c.JobQueue <- tmp
		fmt.Printf("file %v inserted\n", file)
	}

	c.server()
	return &c
}
