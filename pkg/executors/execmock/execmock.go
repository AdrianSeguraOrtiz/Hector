package execmock

import (
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"fmt"
	"math/rand"
	"time"
)

type ExecMock struct{}

func NewExecMock() *ExecMock {
	return &ExecMock{}
}

func (em *ExecMock) ExecuteJob(jobPointer *jobs.Job) (*results.ResultJob, error) {
	/*
		This function simulates the definition of a job.
	*/

	// We print the initialization message and display the job information
	fmt.Printf("Started "+(*jobPointer).Name+" job. Info: \n\t %+v\n\n", *jobPointer)

	// Simulate job definition
	time.Sleep(5 * time.Second)

	// We print the finalization message
	fmt.Println("Finished " + (*jobPointer).Name + " job\n")

	// We return the result of the definition, occasionally simulating the production of an error during it.
	if rand.Float64() < 0.5 {
		return &results.ResultJob{Id: (*jobPointer).Id, Name: (*jobPointer).Name, Logs: "File not found exception", Status: results.Error}, nil
	} else {
		return &results.ResultJob{Id: (*jobPointer).Id, Name: (*jobPointer).Name, Logs: "All right", Status: results.Done}, nil
	}
}
