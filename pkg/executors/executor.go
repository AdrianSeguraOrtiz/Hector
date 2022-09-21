package executors

import (
	"dag/hector/golang/module/pkg/executions"
	"golang.org/x/exp/maps"
	"sync"
)

type Job struct {
	Id 				string
	Name 			string
	Image 			string
	Arguments 		[]executions.Parameter
	Dependencies 	[]string
}

type Status int64
const (
	Waiting Status = iota
	Done
	Error
	Cancelled
)

type ResultJob struct {
	Id 		string
	Name	string
	Logs 	string
	Status 	Status
}

type ResultExecution struct {
	Id			string
    Name		string
	Workflow	string
	Jobs 		[]ResultJob
}


type Executor interface {
	ExecuteJob(jobPointer *Job) ResultJob
}


func ExecuteJobs(nestedJobsPointer *[][]Job, executorPointer *Executor) []ResultJob {
    // We create a map for storing the results of each job
    jobResults := make(map[string]ResultJob)

    // For each group of tasks ...
    for _, jobGroup := range *nestedJobsPointer {

        // We create a waitgroup to allow waiting for all tasks belonging to the group
        var wg sync.WaitGroup

        // For each job in the group ...
        for _, job := range jobGroup {

            // If any of its dependencies has previously failed, the job is cancelled and its execution is dispensed with.
            cancelled := false
            for _, depName := range job.Dependencies {
                if jobResults[depName].Status == Error || jobResults[depName].Status == Cancelled {
                    cancelled = true
                    jobResults[job.Name] = ResultJob{Id: job.Id, Name: job.Name, Logs: "Cancelled due to errors in its dependencies", Status: Cancelled}
                    break
                }
            }

            // If none of its dependencies have previously failed, it is put into execution in a goroutine.
            /** 
                See https://gobyexample.com/waitgroups, 
                https://go.dev/doc/faq#closures_and_goroutines, 
                https://stackoverflow.com/questions/18499352/golang-concurrency-how-to-append-to-the-same-slice-from-different-goroutines
            */
            if !cancelled {
                wg.Add(1)
                go func(j Job) {
                    jobResults[j.Name] = (*executorPointer).ExecuteJob(&j)
                    wg.Done()
                }(job)
            }
        }

        // Wait for all group tasks to be completed before starting the next group
        wg.Wait()
    }

    return maps.Values(jobResults)
}