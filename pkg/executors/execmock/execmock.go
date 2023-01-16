package execmock

import (
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"fmt"
	"math/rand"
	"time"
)

type ExecMock struct{}

/**
NewExecMock function creates a new instance of the ExecMock type. It
returns a pointer to the constructed variable.
*/
func NewExecMock() *ExecMock {
	return &ExecMock{}
}

/**
ExecuteJob function simulates the execution of a job. It takes as input the pointer
of a given Job. It provides as output a pointer to the generated ResultJob and an
error variable in charge of notifying any problem.
*/
func (em *ExecMock) ExecuteJob(job *jobs.Job) (*results.ResultJob, error) {

	// We print the initialization message and display the job information
	fmt.Printf("Started "+job.Name+" job. Info: \n\t %+v\n\n", *job)

	// Simulate job definition
	time.Sleep(5 * time.Second)

	// We print the finalization message
	fmt.Println("Finished " + job.Name + " job\n")

	// We return the result of the definition, occasionally simulating the production of an error during it.
	if rand.Float64() < 0.5 {
		return &results.ResultJob{Id: job.Id, Name: job.Name, Logs: "File not found exception", Status: results.Error}, nil
	} else {
		return &results.ResultJob{Id: job.Id, Name: job.Name, Logs: "All right", Status: results.Done}, nil
	}
}
