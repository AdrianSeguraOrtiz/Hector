package executors

import (
	"dag/hector/golang/module/pkg/databases"
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"sync"

	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

type Executor interface {
	ExecuteJob(jobPointer *jobs.Job) (results.ResultJob, error)
}

func ExecuteJobs(nestedJobsPointer *[][]jobs.Job, executorPointer *Executor, resultDefinitionPointer *results.ResultDefinition, databasePointer *databases.Database) ([]results.ResultJob, error) {
	// We create a map for storing the results of each job (local storage)
	jobResults := make(map[string]results.ResultJob)

	// We created an access control system to prevent co-occurrence into goroutines
	mutex := &sync.RWMutex{}

	// We fill the map with the input Result Definition (remote storage)
	for _, jobRes := range (*resultDefinitionPointer).ResultJobs {
		jobResults[jobRes.Name] = jobRes
	}

	// For each group of tasks ...
	for _, jobGroup := range *nestedJobsPointer {

		// We create an error group to allow waiting for all tasks belonging to the group and collect any error
		var errg errgroup.Group

		// For each job in the group ...
		for _, job := range jobGroup {

			// If the job is not pending execution, it is ignored.
			pending := jobResults[job.Id].Status == results.Waiting

			// If the job must be cancelled, it is ignored.
			cancelled := false

			// For each dependencie job ...
			for _, depName := range job.Dependencies {

				// If any of its dependencies has previously failed, the job is cancelled and its execution is dispensed with.
				if jobResults[depName].Status == results.Error || jobResults[depName].Status == results.Cancelled {

					// Cancel current job
					cancelled = true

					// Create Result Job
					jobRes := results.ResultJob{Id: job.Id, Name: job.Name, Logs: "Cancelled due to errors in its dependencies", Status: results.Cancelled}

					// Save result job in local storage
					jobResults[job.Name] = jobRes

					// Save result job in remote storage
					err := (*databasePointer).UpdateResultJob(&jobRes, (*resultDefinitionPointer).Id)
					if err != nil {
						return nil, err
					}

					// If one of the dependencies has already failed, the search is stopped.
					break
				}
			}

			// If none of its dependencies have previously failed and the job is pending, it is put into execution in a goroutine.
			/**
			  See https://gobyexample.com/waitgroups,
			  https://go.dev/doc/faq#closures_and_goroutines,
			  https://stackoverflow.com/questions/18499352/golang-concurrency-how-to-append-to-the-same-slice-from-different-goroutines
			*/
			if pending && !cancelled {
				j := job
				errg.Go(func() error {
					// Execute job
					jobRes, err := (*executorPointer).ExecuteJob(&j)
					if err != nil {
						return err
					}

					// Save result in local storage (with control access)
					mutex.Lock()
					jobResults[j.Name] = jobRes
					mutex.Unlock()

					// Save result in remote storage
					updateErr := (*databasePointer).UpdateResultJob(&jobRes, (*resultDefinitionPointer).Id)
					if updateErr != nil {
						return updateErr
					}
					return nil
				})
			}
		}

		// Wait for all group tasks to be completed before starting the next group
		err := errg.Wait()
		if err != nil {
			return nil, err
		}
	}

	return maps.Values(jobResults), nil
}
