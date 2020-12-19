package mr

import (
	"strconv"
	"strings"
)

func IntermediateName(nMap int, nReduce int) string {
	parts := []string{"mr", strconv.Itoa(nMap), strconv.Itoa(nReduce)}
	return strings.Join(parts, "-")
}

func OutputName(nReduce int) string {
	parts := []string{"mr", "out", strconv.Itoa(nReduce)}
	return strings.Join(parts, "-")
}
