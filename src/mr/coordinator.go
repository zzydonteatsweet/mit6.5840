package mr

import (
	"encoding/json"
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
	//fmt.Printf("DistributeJob workerId %v \n", *workerId)
	distributeLock.Lock()
	defer distributeLock.Unlock()
	if c.JobPhase == 2 {
		doneJob := Job{}
		doneJob.JobType = 2
		*job = doneJob
		return nil
	}
	*job = *<-c.JobQueue
	jsonStr, _ := json.Marshal(*job)
	fmt.Printf("SentJob Is %v\n", string(jsonStr))
	c.updateJobIsDoing(job.JobId, job)

	fmt.Printf("DistributeJob %v \n", *job)
	return nil
}

func (c *Coordinator) updateJobIsDoing(id int, job *Job) {
	job.JobProgress = 1
	job.StartTime = time.Now()
	if c.JobPhase == 0 {
		c.MapJobList[id].JobProgress = 1
		c.MapJobList[id].StartTime = time.Now()
	} else if c.JobPhase == 1 {
		c.ReduceJobList[id].JobProgress = 1
		c.ReduceJobList[id].StartTime = time.Now()
	}
}

func (c *Coordinator) FinishJob(jobId *int, _ *int) error {
	fmt.Println("server job finished", *jobId)
	distributeLock.Lock()
	defer distributeLock.Unlock()
	if c.JobPhase == 0 {
		c.MapJobList[*jobId].JobProgress = 2
		c.updateJobPhaseFinishedToReduce()
	} else if c.JobPhase == 1 {
		c.ReduceJobList[*jobId].JobProgress = 2
		c.updateJobPhaseFinishedToDone()
	}

	return nil
}

func (c *Coordinator) insertReduceTask() {
	for i := 0; i < c.ReduceCnt; i++ {
		for j := 0; j < c.MapCnt; j++ {
			filename := fmt.Sprintf("mr-tmp-%d-%d.txt", j, i)
			fmt.Printf("Insert Reduce Task FileName is %v \n", filename)
			c.ReduceJobList[i].InputFiles = append(c.ReduceJobList[i].InputFiles, filename)
		}
		c.ReduceJobList[i].ReduceCnt = c.ReduceCnt
		c.ReduceJobList[i].JobType = 1
		c.ReduceJobList[i].JobId = i
		c.ReduceJobList[i].JobProgress = 0
		c.ReduceJobList[i].MapId = i
		c.ReduceJobList[i].MapCnt = c.MapCnt
		c.ReduceJobList[i].ReduceCnt = c.ReduceCnt
		c.JobQueue <- c.ReduceJobList[i]
	}
}

func (c *Coordinator) updateJobPhaseFinishedToReduce() {
	ok := true
	for _, job := range c.MapJobList {
		ok = ok && (job.JobProgress == 2)
	}
	if ok {
		time.Sleep(time.Second)
		c.JobPhase = 1
		c.insertReduceTask()
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

func (c *Coordinator) updateJobPhaseFinishedToDone() {
	ok := true
	for i := 0; i < c.ReduceCnt; i++ {
		if c.ReduceJobList[i].JobProgress != 2 {
			fmt.Printf(`Check Job For Finish Doing Job is %v
`, c.ReduceJobList[i])
			ok = false
			break
		}
	}
	if ok {
		for i, v := range c.ReduceJobList {
			fmt.Printf("Finished %v th Reduce Job %v\n", i, *v)
		}
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

	for i := 0; i < nReduce; i++ {
		c.ReduceJobList = append(c.ReduceJobList, new(Job))
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
