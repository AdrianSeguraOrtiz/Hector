package execmock

import (
	"fmt"
	"time"
	"dag/hector/golang/module/pkg/executors"
)

type ExecMock struct {}

func (em *ExecMock) ExecuteTask(task *executors.RunTask) {
	/*
		This function simulates the execution of a task.
	*/
	fmt.Println("Executing " + task.Name + " task")
}

func (em *ExecMock) Wait(id string) bool {
	/*
		This function simulates waiting for a task
	*/
	
	time.Sleep(5 * time.Second)
	return true
}