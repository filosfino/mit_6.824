package mr

import (
	"os"
	"strconv"
	"time"
)

func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func randStr() string {
	now := time.Now()
	return strconv.Itoa(int(now.UnixNano()))
}

func workerSock(name string) string {
	return "/var/tmp/824-mr-worker-" + name
}
