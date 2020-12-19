package mr

import (
	"sync"
	"time"
)

const (
	Map    = "Map"
	Reduce = "Reduce"
)

const (
	Pending    = "Pending"
	InProgress = "InProgress"
	Done       = "Done"
)

// TaskType is "Map" or "Reduce"
type TaskType string

// TaskData is the task info
type TaskData struct {
	Id      int
	T       TaskType
	File    string
	NReduce int
	NMap    int
}

// TaskResult is the task result
type TaskResult struct {
	*TaskData
	Result bool
}

type TaskDoneArgs struct {
	TaskData
	Success bool
}

type TaskDoneReply struct {
	Ack bool
}

type TaskWithStatus struct {
	*TaskData
	Status    string
	StartedAt *time.Time
	DoneAt    *time.Time
}

// Master master data
type Master struct {
	Tasks    []TaskWithStatus
	taskChan chan TaskData
	Stage    string
	nReduce  int
	nMap     int
	Files    []string
	mutex    sync.Mutex
}

type ClaimTaskArgs struct {
}

type ClaimTaskReply struct {
	TaskData
	Done bool
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
