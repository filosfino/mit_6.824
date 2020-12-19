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
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task := claimTask()
		if task == nil {
			fmt.Printf("task 已处理完毕\n")
			return
		} else if task.T == Map {
			err := doMap(mapf, task)
			if err == nil {
				reportTaskDone(task)
			}
		} else if task.T == Reduce {
			err := doReduce(reducef, task)
			if err == nil {
				reportTaskDone(task)
			}
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, task *TaskData) error {
	filename := task.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}
	file.Close()
	kva := mapf(filename, string(content))

	for reduceIndex := 0; reduceIndex < task.NReduce; reduceIndex++ {
		intermediateFileName := IntermediateName(task.Id, reduceIndex)
		file, err := os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot create %v", intermediateFileName)
			return err
		}
		enc := json.NewEncoder(file)
		for _, kv := range kva {
			if ihash(kv.Key)%task.NReduce == reduceIndex {
				enc.Encode(&kv)
			}
		}
		file.Close()
	}
	return nil
}

func doReduce(reducef func(string, []string) string, task *TaskData) error {
	reduceIndex := task.Id - task.NMap
	res := make(map[string][]string)
	for i := 0; i < task.NMap; i++ {
		intermediateFileName := IntermediateName(i, reduceIndex)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFileName)
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := res[kv.Key]
			if !ok {
				res[kv.Key] = make([]string, 0)
			}
			res[kv.Key] = append(res[kv.Key], kv.Value)
		}
		file.Close()
	}

	var keys []string
	for k := range res {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	outputFileName := OutputName(reduceIndex)
	outputFile, _ := os.Create(outputFileName)
	for _, k := range keys {
		output := reducef(k, res[k])
		fmt.Fprintf(outputFile, "%v %v\n", k, output)
	}
	outputFile.Close()
	return nil
}

func claimTask() *TaskData {
	args := &ClaimTaskArgs{}
	reply := &ClaimTaskReply{}
	fmt.Printf("[worker] claiming task\n")
	call("Master.HandleClaimTask", &args, &reply)
	if reply.Done {
		return nil
	}
	fmt.Printf("[worker] received claming task reply %v\n", *reply.TaskData)
	return reply.TaskData
}

func reportTaskDone(task *TaskData) error {
	args := &TaskDoneArgs{
		TaskData: task,
		Success:  true,
	}
	reply := &TaskDoneReply{}
	call("Master.HandleTaskDone", &args, &reply)
	fmt.Printf("[worker] reporting task done\n")
	return nil
}

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
