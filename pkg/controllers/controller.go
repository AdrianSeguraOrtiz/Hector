package controllers

import (
	"dag/hector/golang/module/pkg/databases"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/errors"
	"dag/hector/golang/module/pkg/executors"
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"dag/hector/golang/module/pkg/schedulers"
	"dag/hector/golang/module/pkg/specifications"
	"dag/hector/golang/module/pkg/validators"
	"fmt"
	"log"
	"sync"

	"github.com/rs/xid"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

type Controller struct {
	Executor  *executors.Executor
	Scheduler *schedulers.Scheduler
	Database  *databases.Database
	Validator *validators.Validator
}

func (c *Controller) Invoke(definition *definitions.Definition) (*results.ResultDefinition, error) {
	/**
	This function is responsible for the complete execution of a given definition.
	*/

	// Get jobs in topological order thanks to the scheduler while simultaneously validating the tasks
	// and parameters exposed in the definition (must be compatible with the corresponding specification).
	nestedJobs, err := getJobs(definition, c.Database, c.Validator)
	if err != nil {
		return nil, fmt.Errorf("error while trying to get jobs %s", err.Error())
	}

	// If the definition already has a result in the database we download it.
	resultDefinition, err := (*c.Database).GetResultDefinition((*definition).Id)

	// Otherwise we create an empty one, set all its jobs to waiting and upload it to the database before starting the execution.
	switch err.(type) {
	case *errors.ElementNotFoundErr:
		{
			// We inform the user that a new result has been created in the database.
			log.Printf(err.Error() + " A new document is created.")

			// Create empty result definition
			resultDefinition = &results.ResultDefinition{
				Id:              (*definition).Id,
				Name:            (*definition).Name,
				SpecificationId: (*definition).SpecificationId,
				ResultJobs:      []results.ResultJob{},
			}

			// We instantiate all its works in a waiting state
			for _, jobGroup := range nestedJobs {
				for _, job := range jobGroup {
					(*resultDefinition).ResultJobs = append((*resultDefinition).ResultJobs, results.ResultJob{Id: job.Id, Name: job.Name, Status: results.Waiting})
				}
			}

			// We add the result definition to the database
			err := (*c.Database).AddResultDefinition(resultDefinition)
			if err != nil {
				return nil, fmt.Errorf("error during insertion into the database %s", err.Error())
			}
		}
	default:
		// In case of another error, it is returned in the function
		if err != nil {
			return nil, err
		}
	}

	// Execute jobs
	(*resultDefinition).ResultJobs, err = executeJobs(&nestedJobs, c.Executor, resultDefinition, c.Database)
	if err != nil {
		return nil, fmt.Errorf("error during execution %s", err.Error())
	}

	// We return the pointer to the constructed result definition
	return resultDefinition, nil
}

func getJobs(definition *definitions.Definition, database *databases.Database, validator *validators.Validator) ([][]jobs.Job, error) {
	/**
	This function is responsible for extracting the jobs (minimum units of information for an execution)
	in the order established by the scheduler. In addition, during the process it is in charge of validating
	the consistency between the definition and the specification and components.
	*/

	// We extract the associated specification and its topological order
	specification, err := (*database).GetSpecification((*definition).SpecificationId)
	if err != nil {
		return nil, err
	}
	planning, err := (*database).GetPlanning((*definition).SpecificationId)
	if err != nil {
		return nil, err
	}

	// We validate that the tasks required in the specification are specified in the definition file
	taskValidatorErr := (*validator).ValidateDefinitionTaskNames(&(*definition).Data.Tasks, &(*specification).Spec.Dag.Tasks)
	if taskValidatorErr != nil {
		return nil, taskValidatorErr
	}

	// We build a two-dimensional vector to store the topologically ordered tasks with the necessary content for their definition.
	var nestedJobs [][]jobs.Job

	// For each group of tasks ...
	for _, taskGroup := range *planning {

		// One-dimensional vector for storing group tasks
		var jobsGroup []jobs.Job

		// For each task within the group ...
		for _, taskName := range taskGroup {

			// A. We extract the task information from the definition file
			idxDefinitionTask := slices.IndexFunc((*definition).Data.Tasks, func(t definitions.DefinitionTask) bool { return t.Name == taskName })
			definitionTask := (*definition).Data.Tasks[idxDefinitionTask]

			// B. We extract the task information from the specification struct (mainly to know the identifier of its component)
			idxSpecificationTask := slices.IndexFunc((*specification).Spec.Dag.Tasks, func(t specifications.SpecificationTask) bool { return t.Name == taskName })
			specificationTask := (*specification).Spec.Dag.Tasks[idxSpecificationTask]
			componentId := specificationTask.Component

			// C. We extract the information about the task component
			execComponent, err := (*database).GetComponent(componentId)
			if err != nil {
				return nil, err
			}

			// D. We check that the parameters entered (inputs/outputs) in the definition file are correct
			inputValidatorErr := (*validator).ValidateDefinitionParameters(&definitionTask.Inputs, &(*execComponent).Inputs)
			if inputValidatorErr != nil {
				return nil, inputValidatorErr
			}
			outputValidatorErr := (*validator).ValidateDefinitionParameters(&definitionTask.Outputs, &(*execComponent).Outputs)
			if outputValidatorErr != nil {
				return nil, outputValidatorErr
			}

			// E. We create the definition task (job)
			job := jobs.Job{
				Id:           xid.New().String(),
				Name:         taskName,
				Image:        (*execComponent).ContainerImage,
				Arguments:    append(definitionTask.Inputs, definitionTask.Outputs...),
				Dependencies: specificationTask.Dependencies,
			}

			// F. We add it to the group's task list
			jobsGroup = append(jobsGroup, job)
		}
		// We add the group's tasks to the two-dimensional list
		nestedJobs = append(nestedJobs, jobsGroup)
	}

	return nestedJobs, nil
}

func executeJobs(nestedJobs *[][]jobs.Job, executor *executors.Executor, resultDefinition *results.ResultDefinition, database *databases.Database) ([]results.ResultJob, error) {
	/**
	This function is responsible for executing the jobs in the order established in the
	two-dimensional list. In addition, it stores real-time information in the database
	in order to facilitate the resolution of cuts during execution.
	*/

	// We create a map for storing the results of each job (local storage)
	jobResults := make(map[string]results.ResultJob)

	// We created an access control system to prevent co-occurrence into goroutines
	mutex := &sync.RWMutex{}

	// We fill the map with the input Result Definition (remote storage)
	for _, jobRes := range (*resultDefinition).ResultJobs {
		jobResults[jobRes.Name] = jobRes
	}

	// For each group of tasks ...
	for _, jobGroup := range *nestedJobs {

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
					err := (*database).UpdateResultJob(&jobRes, (*resultDefinition).Id)
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
					jobRes, err := (*executor).ExecuteJob(&j)
					if err != nil {
						return err
					}

					// Save result in local storage (with control access)
					mutex.Lock()
					jobResults[j.Name] = *jobRes
					mutex.Unlock()

					// Save result in remote storage
					updateErr := (*database).UpdateResultJob(jobRes, (*resultDefinition).Id)
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
