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
	// 0 正常 1 异常
	WorkerState []int
}

type Job struct {
	JobId int
	//  0 Not Started 1 Doing 2 Finish
	JobProgress int
	// 0 Map 1 Reduce 2 Finished 3 Wait
	JobType    int
	InputFiles []string
	StartTime  time.Time
	WorkerId   int
	ReduceCnt  int
	MapCnt     int
}

func (c *Coordinator) DistributeJob(workerId *int, job *Job) error {
	//fmt.Printf("DistributeJob workerId %v \n", *workerId)
	distributeLock.Lock()
	defer distributeLock.Unlock()
	if -1 != *workerId {
		if c.WorkerState[*workerId] == 1 {
			fmt.Printf("GET Wrong Worker Id %d\n", *workerId)
			job.JobType = 2
			return nil
		}
	}

	if c.JobPhase == 2 {
		doneJob := Job{}
		doneJob.JobType = 2
		*job = doneJob
		return nil
	}

	if -1 == *workerId {
		*workerId = len(c.WorkerState)
		c.WorkerState = append(c.WorkerState, 0)
		job.WorkerId = *workerId
		//fmt.Printf("Got a not register Worker, now WorkerId %v\n", *workerId)
	}

	if len(c.JobQueue) > 0 {
		*job = *<-c.JobQueue
		if c.JobPhase == 0 {
			c.MapJobList[job.JobId].WorkerId = *workerId
		} else if c.JobPhase == 1 {
			c.ReduceJobList[job.JobId].WorkerId = *workerId
		}

		//jsonStr, _ := json.Marshal(*job)
		//fmt.Printf("SentJob Is %v\n", string(jsonStr))
		c.updateJobIsDoing(job.JobId, job)
	} else {
		job.JobType = 3
	}

	//fmt.Printf("DistributeJob %v \n", *job)
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
	//fmt.Println("server job finished", *jobId)
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
			//fmt.Printf("Insert Reduce Task FileName is %v \n", filename)
			c.ReduceJobList[i].InputFiles = append(c.ReduceJobList[i].InputFiles, filename)
		}
		c.ReduceJobList[i].ReduceCnt = c.ReduceCnt
		c.ReduceJobList[i].JobType = 1
		c.ReduceJobList[i].JobId = i
		c.ReduceJobList[i].JobProgress = 0
		c.ReduceJobList[i].WorkerId = i
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

func (c *Coordinator) checkMapWorkerNormal() {
	currentTime := time.Now()
	for i := 0; i < len(c.MapJobList); i++ {
		if c.MapJobList[i].JobProgress == 1 && currentTime.Sub(c.MapJobList[i].StartTime) > 5*time.Second {
			fmt.Printf("detect a crash on map job %v worker id %v\n", c.MapJobList[i].JobId, c.MapJobList[i].WorkerId)
			c.MapJobList[i].JobProgress = 0
			c.WorkerState[c.MapJobList[i].WorkerId] = 1
			c.JobPhase = 0
			c.JobQueue <- c.MapJobList[i]
		}
	}
}

func (c *Coordinator) checkReduceWorkerStateNormal() {
	currentTime := time.Now()
	for i := 0; i < len(c.ReduceJobList); i++ {
		if c.ReduceJobList[i].JobProgress == 1 && currentTime.Sub(c.ReduceJobList[i].StartTime) > 5*time.Second {
			fmt.Printf("detect a crash on reduce job %v worker id %v\n", c.ReduceJobList[i].JobId, c.ReduceJobList[i].WorkerId)
			c.ReduceJobList[i].JobProgress = 0
			c.WorkerState[c.ReduceJobList[i].WorkerId] = 1
			c.JobPhase = 1
			c.JobQueue <- c.ReduceJobList[i]
		}
	}
}

func (c *Coordinator) updateJobPhaseFinishedToDone() {
	ok := true
	for i := 0; i < c.ReduceCnt; i++ {
		if c.ReduceJobList[i].JobProgress != 2 {
			//fmt.Printf(`Check Job For Finish Doing Job is %v`, c.ReduceJobList[i])
			ok = false
			break
		}
	}
	if ok {
		//for i, v := range c.ReduceJobList {
		//fmt.Printf("Finished %v th Reduce Job %v\n", i, *v)
		//}
		c.JobPhase = 2
	}
}

func (c *Coordinator) checkWorkerCrashed() {
	for {
		time.Sleep(time.Second * 2)
		distributeLock.Lock()
		if c.JobPhase == 0 {
			c.checkMapWorkerNormal()
		} else if c.JobPhase == 1 {
			fmt.Println("checked Reduce Phase")
			c.checkReduceWorkerStateNormal()
		}
		distributeLock.Unlock()
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//fmt.Println("making coordinator...")
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
		//fmt.Println("insert file ", file)
		tmp := new(Job)
		tmp.JobType = 0
		tmp.InputFiles = []string{}
		tmp.InputFiles = append(tmp.InputFiles, file)
		tmp.ReduceCnt = nReduce
		tmp.JobId = i
		tmp.WorkerId = i
		tmp.MapCnt = len(files)
		c.MapJobList = append(c.MapJobList, tmp)
		c.JobQueue <- tmp
		//fmt.Printf("file %v inserted\n", file)
	}
	go c.checkWorkerCrashed()
	c.server()
	return &c
}
