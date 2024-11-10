package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

var workerId = 0

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	fmt.Printf("Worker started.\n")
	var tmp = 0
	ok := true
	for ok {
		job := Job{}
		ok = call("Coordinator.DistributeJob", &tmp, &job)
		//jsonStr, _ := json.Marshal(job)
		//fmt.Printf("Receive job is %v\n", string(jsonStr))
		if job.JobType == 0 {
			err := doMap(&job, mapf)
			if err != nil {
				fmt.Printf("Worker Map processing %v Error %v:\n", job.JobId, err)
				return
			}
		} else if job.JobType == 1 {
			err := doReduce(&job, reducef)
			if err != nil {
				fmt.Printf("Worker Reduce processing %v Error %v:\n", job.JobId, err)
				return
			}
		} else if job.JobType == 2 {
			fmt.Printf("All Work is Done\n")
			break
		} else {
			time.Sleep(time.Second)
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doReduce(job *Job, reduef func(string, []string) string) error {
	var kva []KeyValue
	for _, filename := range job.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			dir, _ := os.Getwd()
			fmt.Printf("Error opening file name is %v error is %v path is %v", filename, err, dir)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		err = file.Close()
		if err != nil {
			dir, _ := os.Getwd()
			fmt.Printf("Close file error name is %v error is %v path is %v\n", filename, err, dir)
		}
	}

	sort.Sort(ByKey(kva))

	dir, _ := os.Getwd()
	tmpFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		fmt.Printf("TempFile Error:%v\n", err)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		key := kva[i].Key
		values := []string{}
		for ; i < j; i++ {
			values = append(values, kva[i].Value)
		}
		output := reduef(key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", key, output)
	}
	tmpFile.Close()
	oname := fmt.Sprintf("mr-out-%v.txt", job.JobId)
	os.Rename(tmpFile.Name(), oname)
	jobIsDone(job.JobId)
	return nil
}

func doMap(job *Job, mapf func(string, string) []KeyValue) error {
	//fmt.Printf("Map Started Job is %v\n", *job)
	intermediates := make([][]KeyValue, job.ReduceCnt)
	reduceCnt := job.ReduceCnt
	for _, filename := range job.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Map cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		err = file.Close()
		if err != nil {
			//fmt.Printf("Map Close file error filename is %v message is %v\n", filename, err)
		}
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			loc := ihash(kv.Key) % reduceCnt
			intermediates[loc] = append(intermediates[loc], kv)
		}
	}
	for i, intermediate := range intermediates {
		//fmt.Printf("len of intermediates %v is %v\n", i, len(intermediates[i]))

		interFileName := "mr-tmp-" + strconv.Itoa(job.JobId) + "-" + strconv.Itoa(i) + ".txt"
		ofile, _ := os.Create(interFileName)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate {
			enc.Encode(kv)
		}
		err := ofile.Close()
		//fmt.Println("create file and insert finished " + interFileName)
		if err != nil {
			return err
		}
	}
	jobIsDone(job.JobId)
	return nil
}

func jobIsDone(jobId int) {
	//fmt.Printf("SendMessage Job %v Start.\n", jobId)
	state := 0
	err := call("Coordinator.FinishJob", &jobId, &state)
	if err != true {
		fmt.Printf("Worker SendMessage Job %v Error:%v\n", jobId, err)
	}
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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
