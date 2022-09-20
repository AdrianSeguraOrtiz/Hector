package execmock

import (
	"fmt"
	"time"
	"math/rand"
	"dag/hector/golang/module/pkg/executors"
)

type ExecMock struct {}

func (em *ExecMock) ExecuteJob(jobPointer *executors.Job) executors.Result {
	/*
		This function simulates the execution of a job.
	*/

	// We print the initialization message and display the job information
	fmt.Printf("Started " + (*jobPointer).Name + " job. Info: \n\t %+v\n\n", *jobPointer)

	// Simulate job execution
	time.Sleep(5 * time.Second)

	// We print the finalization message
	fmt.Println("Finished " + (*jobPointer).Name + " job\n")

	// We return the result of the execution, occasionally simulating the production of an error during it.
	if (rand.Float64() < 0.5) {
		return executors.Result{Id: (*jobPointer).Id, Logs: "File not found exception", Status: executors.Error}
	} else {
		return executors.Result{Id: (*jobPointer).Id, Logs: "All right", Status: executors.Done}
	}
}