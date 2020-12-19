package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

func (m *Master) TasksFinished() bool {
	for _, task := range m.Tasks {
		if task.TaskData == nil {
			break
		}
		if task.Status != Done {
			return false
		}
	}
	return true
}

// 封装获取任务
func (m *Master) GetNextTask() *TaskData {
	for {
		task, ok := <-m.taskChan
		fmt.Printf("[master] GenNextTask %v\n", task)
		if !ok {
			return nil
		} else {
			return &task
		}
	}
}

func (m *Master) InitTaskChan() {
	fmt.Printf("[master] InitTaskChan %v\n", m.Tasks)
	if m.Stage == Map {
		for i, file := range m.Files {
			m.Tasks[i] = TaskWithStatus{
				TaskData: &TaskData{
					Id:      i,
					T:       Map,
					File:    file,
					NReduce: m.nReduce,
					NMap:    m.nMap,
				},
				Status: Pending,
			}
		}
		go func() {
			for _, task := range m.Tasks[:m.nMap] {
				m.taskChan <- *task.TaskData
			}
		}()
	}
	if m.Stage == Reduce {
		for i := 0; i < m.nReduce; i++ {
			m.Tasks[m.nMap+i] = TaskWithStatus{
				TaskData: &TaskData{
					Id:      m.nMap + i,
					T:       Reduce,
					File:    "",
					NReduce: m.nReduce,
					NMap:    m.nMap,
				},
				Status: Pending,
			}
		}
		go func() {
			for _, task := range m.Tasks[m.nMap:] {
				m.taskChan <- *task.TaskData
			}
		}()
	}
}

func (m *Master) HandleClaimTask(args *ClaimTaskArgs, reply *ClaimTaskReply) error {
	task := m.GetNextTask()
	fmt.Printf("[master] worker requesting task\n")
	if task == nil {
		fmt.Printf("[master] all tasks released\n")
		reply.Done = true
		// 没有更多任务了
	} else {
		fmt.Printf("[master] releasing task %v\n", *task)
		now := time.Now()
		m.Tasks[task.Id].StartedAt = &now
		m.Tasks[task.Id].Status = InProgress
		fmt.Printf("[master] worker requested task %v\n", *m.Tasks[task.Id].TaskData)
		go func() {
			time.Sleep(10 * time.Second)
			if m.Tasks[task.Id].Status != Done {
				m.RescheduleTask(task.Id)
			}
		}()
		reply.TaskData = m.Tasks[task.Id].TaskData
		reply.Done = false
	}
	return nil
}

func (m *Master) UpdateStatus(id int, status string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Tasks[id].Status = status
	if status == Done {
		now := time.Now()
		m.Tasks[id].DoneAt = &now
		finished := m.TasksFinished()
		if finished == true {
			fmt.Printf("[master] stage %v finished\n", m.Stage)
			if m.Stage == Map {
				// 启动 Reduce 模式
				m.Stage = Reduce
				fmt.Printf("[master] stage %v starting\n", m.Stage)
				m.InitTaskChan()
			} else if m.Stage == Reduce {
				fmt.Printf("[master] job done\n")
				close(m.taskChan)
			}
		}
	}
	if status == Pending {
		m.Tasks[id].Status = Pending
		m.Tasks[id].StartedAt = nil
	}
}

func (m *Master) RescheduleTask(id int) {
	m.UpdateStatus(id, Pending)
	fmt.Printf("[master] rescheduling task %d\n", id)
	m.taskChan <- *m.Tasks[id].TaskData
}

func (m *Master) HandleTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	fmt.Printf("[master] worker reporting task %d status %v\n", args.TaskData.Id, args.Success)
	if args.Success == true {
		m.UpdateStatus(args.Id, Done)
	} else {
		m.RescheduleTask(args.Id)
	}
	reply.Ack = true
	return nil
}

// RPC register
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	fmt.Printf("[master] server is on\n")
}

// Done is called periodically to find out if entire job is done
func (m *Master) Done() bool {
	ret := m.TasksFinished()
	return ret
}

// MakeMaster starts master
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.Stage = Map
	m.nReduce = nReduce
	m.nMap = len(files)
	m.Files = files
	m.taskChan = make(chan TaskData)
	m.Tasks = make([]TaskWithStatus, m.nMap+m.nReduce)
	// m.mutex = sync.Mutex{}
	m.InitTaskChan()

	m.server()
	return &m
}
