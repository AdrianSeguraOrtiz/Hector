package executors

import (
	"dag/hector/golang/module/pkg/executions"
)

type Job struct {
	Id string
	Name string
	Image string
	Arguments []executions.Parameter
	Dependencies []string
}

type Status int64
const (
	Waiting Status = iota
	Done
	Error
	Cancelled
)

type Result struct {
	Id string
	Logs string
	Status Status
}

type Executor interface {
	ExecuteJob(jobPointer *Job)
}