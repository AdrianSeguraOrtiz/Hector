package executors

import (
	"dag/hector/golang/module/pkg/executions"
)

type RunTask struct {
	Id string
	Name string
	Image string
	Arguments []executions.Parameter
}

type Executor interface {
	ExecuteTask(task *RunTask)
	Wait(id string) bool
}