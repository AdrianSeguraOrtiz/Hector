package mock

import (
	"fmt"
	"time"
	"dag/hector/golang/module/executors"
)

type Mock struct {}

func (m *Mock) ExecuteTask(task *executors.RunTask) {
	fmt.Println("Executing " + task.Name + " task")
}

func (m *Mock) Wait(id string) bool {
	time.Sleep(5 * time.Second)
	return true
}