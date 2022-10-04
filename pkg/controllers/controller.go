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

func NewController(tool string, strategy string, repo string) (*Controller, error) {
	ex, err := executors.NewExecutor(tool)
	if err != nil {
		return nil, err
	}

	sc, err := schedulers.NewScheduler(strategy)
	if err != nil {
		return nil, err
	}

	db, err := databases.NewDatabase(repo)
	if err != nil {
		return nil, err
	}

	val := validators.NewValidator()

	return &Controller{Executor: ex, Scheduler: sc, Database: db, Validator: val}, nil
}

func (c *Controller) Invoke(definitionPointer *definitions.Definition) (*results.ResultDefinition, error) {
	// Get jobs in topological order
	nestedJobs, err := getJobs(definitionPointer, c.Database, c.Validator)
	if err != nil {
		return nil, fmt.Errorf("Error while trying to get jobs", err.Error())
	}

	// Get/Create definition result
	resultDefinition, err := (*c.Database).GetResultDefinition((*definitionPointer).Id)
	switch err.(type) {
	case *errors.ElementNotFoundErr:
		{
			log.Printf(err.Error() + " A new document is created.")
			resultDefinition = results.ResultDefinition{
				Id:              (*definitionPointer).Id,
				Name:            (*definitionPointer).Name,
				SpecificationId: (*definitionPointer).SpecificationId,
				ResultJobs:      []results.ResultJob{},
			}
			err := (*c.Database).AddResultDefinition(&resultDefinition)
			if err != nil {
				return nil, fmt.Errorf("Error during insertion into the database", err.Error())
			}
		}
	default:
		if err != nil {
			return nil, err
		}
	}

	// Execute jobs
	resultDefinition.ResultJobs, err = executeJobs(&nestedJobs, c.Executor, &resultDefinition, c.Database)
	if err != nil {
		return nil, fmt.Errorf("Error during execution", err.Error())
	}

	return &resultDefinition, nil
}

func getJobs(definitionPointer *definitions.Definition, databasePointer *databases.Database, validatorPointer *validators.Validator) ([][]jobs.Job, error) {
	// We extract the associated specification and its topological order
	specification, err := (*databasePointer).GetSpecification((*definitionPointer).SpecificationId)
	if err != nil {
		return nil, err
	}
	planning, err := (*databasePointer).GetTopologicalSort((*definitionPointer).SpecificationId)
	if err != nil {
		return nil, err
	}

	// We validate that the tasks required in the specification are specified in the definition file
	taskValidatorErr := (*validatorPointer).ValidateDefinitionTaskNames(&(*definitionPointer).Data.Tasks, &specification.Spec.Dag.Tasks)
	if taskValidatorErr != nil {
		return nil, taskValidatorErr
	}

	// We build a two-dimensional vector to store the topologically ordered tasks with the necessary content for their definition.
	var nestedJobs [][]jobs.Job

	// For each group of tasks ...
	for _, taskGroup := range planning {

		// One-dimensional vector for storing group tasks
		var jobsGroup []jobs.Job

		// For each task within the group ...
		for _, taskName := range taskGroup {

			// A. We extract the task information from the definition file
			idxDefinitionTask := slices.IndexFunc((*definitionPointer).Data.Tasks, func(t definitions.DefinitionTask) bool { return t.Name == taskName })
			definitionTask := (*definitionPointer).Data.Tasks[idxDefinitionTask]

			// B. We extract the task information from the specification struct (mainly to know the identifier of its component)
			idxSpecificationTask := slices.IndexFunc(specification.Spec.Dag.Tasks, func(t specifications.SpecificationTask) bool { return t.Name == taskName })
			specificationTask := specification.Spec.Dag.Tasks[idxSpecificationTask]
			componentId := specificationTask.Component

			// C. We extract the information about the task component
			execComponent, err := (*databasePointer).GetComponent(componentId)
			if err != nil {
				return nil, err
			}

			// D. We check that the parameters entered (inputs/outputs) in the definition file are correct
			inputValidatorErr := (*validatorPointer).ValidateDefinitionParameters(&definitionTask.Inputs, &execComponent.Inputs)
			if inputValidatorErr != nil {
				return nil, inputValidatorErr
			}
			outputValidatorErr := (*validatorPointer).ValidateDefinitionParameters(&definitionTask.Outputs, &execComponent.Outputs)
			if outputValidatorErr != nil {
				return nil, outputValidatorErr
			}

			// E. We create the definition task (job)
			job := jobs.Job{
				Id:           xid.New().String(),
				Name:         taskName,
				Image:        execComponent.Container.Image,
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

func executeJobs(nestedJobsPointer *[][]jobs.Job, executorPointer *executors.Executor, resultDefinitionPointer *results.ResultDefinition, databasePointer *databases.Database) ([]results.ResultJob, error) {
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
