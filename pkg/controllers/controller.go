package controllers

import (
	"dag/hector/golang/module/pkg/datastores"
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
	Datastore *datastores.Datastore
	Validator *validators.Validator
}

// Invoke function is responsible for the complete execution of a given definition. Takes as input
// the pointer to a Definition variable. Returns the pointer to a ResultDefinition variable and an
// error variable to report any problems.
func (c *Controller) Invoke(definition *definitions.Definition) (*results.ResultDefinition, error) {

	// Get jobs in topological order thanks to the scheduler while simultaneously validating the tasks
	// and parameters exposed in the definition (must be compatible with the corresponding specification).
	nestedJobs, err := getJobs(definition, c.Datastore, c.Validator)
	if err != nil {
		return nil, fmt.Errorf("error while trying to get jobs %s", err.Error())
	}

	// Get result definition or create a default one if it doesn't exist
	resultDefinition, err := getOrDefaultResultDefinition(definition, c.Datastore, nestedJobs)
	if err != nil {
		return nil, fmt.Errorf("error getting result definition %s", err.Error())
	}

	// Execute jobs
	resultJobs, err := executeJobs(nestedJobs, c.Executor, resultDefinition, c.Datastore)
	if err != nil {
		return nil, fmt.Errorf("error during execution %s", err.Error())
	}
	resultDefinition.ResultJobs = *resultJobs

	// We return the pointer to the constructed result definition
	return resultDefinition, nil
}

// getJobs function is responsible for extracting the jobs (minimum units of information for an execution)
// in the order established by the scheduler. In addition, during the process it is in charge of validating
// the consistency between the definition and the specification and components. It takes as input the pointer
// of a Definition variable, the pointer of a Datastore variable and the pointer of a Validator variable. Finally,
// it returns the pointer to a two-dimensional array of Jobs and an error variable to notify of any problem.
func getJobs(definition *definitions.Definition, datastore *datastores.Datastore, validator *validators.Validator) (*[][]jobs.Job, error) {

	// Obtain specification and planning, and validate the concordance between their tasks with respect to those recorded in the definition.
	specification, planning, err := getAndCheckSpecPlanning(definition, datastore, validator)
	if err != nil {
		return nil, err
	}

	// We build a two-dimensional vector to store the topologically ordered tasks with the necessary content for their definition.
	var nestedJobs [][]jobs.Job

	// For each group of tasks ...
	for _, taskGroup := range *planning {

		// One-dimensional vector for storing group tasks
		var jobsGroup []jobs.Job

		// For each task within the group ...
		for _, taskName := range taskGroup {

			// Obtain the work associated with the specified task and validate its parameters with respect to the established in the specification and components.
			job, err := getAndCheckJob(definition, taskName, specification, datastore, validator)
			if err != nil {
				return nil, err
			}

			// F. We add it to the group's task list
			jobsGroup = append(jobsGroup, *job)
		}
		// We add the group's tasks to the two-dimensional list
		nestedJobs = append(nestedJobs, jobsGroup)
	}

	return &nestedJobs, nil
}

// getAndCheckSpecPlanning function is responsible for obtaining the specification and planning associated with
// a definition and validating the concordance between its tasks and those recorded in the definition. It takes
// as input the pointer of a Definition variable, the pointer of a Datastore variable and the pointer of a
// Validator variable. Returns the specification pointer, the pointer to the two-dimensional array representing
// the planning and an error variable to report any problems.
func getAndCheckSpecPlanning(definition *definitions.Definition, datastore *datastores.Datastore, validator *validators.Validator) (*specifications.Specification, *[][]string, error) {

	// We extract the associated specification and its topological order
	specification, err := (*datastore).GetSpecification(definition.SpecificationId)
	if err != nil {
		return nil, nil, err
	}
	planning, err := (*datastore).GetPlanning(definition.SpecificationId)
	if err != nil {
		return nil, nil, err
	}

	// We validate that the tasks required in the specification are specified in the definition file
	taskValidatorErr := validator.ValidateDefinitionTaskNames(&definition.Data.Tasks, &specification.Spec.Dag.Tasks)
	if taskValidatorErr != nil {
		return nil, nil, taskValidatorErr
	}

	return specification, planning, nil
}

// getAndCheckJob function is responsible for constructing the job associated with the specified task as
// well as validating the consistency between the parameters of the definition with respect to what is
// established in the specification and components. It takes as input the pointer of a Definition variable,
// the task name, the pointer of a Specification variable, the pointer of a Datastore variable and the pointer of a
// Validator variable. Returns the pointer to the constructed Job and an error variable to report any problems.
func getAndCheckJob(definition *definitions.Definition, taskName string, specification *specifications.Specification, datastore *datastores.Datastore, validator *validators.Validator) (*jobs.Job, error) {

	// A. We extract the task information from the definition file
	idxDefinitionTask := slices.IndexFunc(definition.Data.Tasks, func(t definitions.DefinitionTask) bool { return t.Name == taskName })
	definitionTask := definition.Data.Tasks[idxDefinitionTask]

	// B. We extract the task information from the specification struct (mainly to know the identifier of its component)
	idxSpecificationTask := slices.IndexFunc(specification.Spec.Dag.Tasks, func(t specifications.SpecificationTask) bool { return t.Name == taskName })
	specificationTask := specification.Spec.Dag.Tasks[idxSpecificationTask]
	componentId := specificationTask.Component

	// C. We extract the information about the task component
	execComponent, err := (*datastore).GetComponent(componentId)
	if err != nil {
		return nil, err
	}

	// D. We check that the parameters entered (inputs/outputs) in the definition file are correct
	inputValidatorErr := validator.ValidateDefinitionParameters(&definitionTask.Inputs, &execComponent.Inputs)
	if inputValidatorErr != nil {
		return nil, inputValidatorErr
	}
	outputValidatorErr := validator.ValidateDefinitionParameters(&definitionTask.Outputs, &execComponent.Outputs)
	if outputValidatorErr != nil {
		return nil, outputValidatorErr
	}

	// E. We create the definition task (job)
	job := &jobs.Job{
		Id:           xid.New().String(),
		Name:         taskName,
		Image:        execComponent.ContainerImage,
		Arguments:    append(definitionTask.Inputs, definitionTask.Outputs...),
		Dependencies: specificationTask.Dependencies,
	}

	return job, nil
}

// getOrDefaultResultDefinition function is responsible for downloading the execution result
// recorded in the datastore for the specified definition. In case it has not been executed
// before, it will not find any result in the datastore and will create a new one with the
// default values. It takes as input the pointer of a Definition variable, the pointer of a
// Datastore variable and the pointer of set of jobs in topological order. Returns the pointer
// to the RestultDefinition variable and an error variable to report any problems.
func getOrDefaultResultDefinition(definition *definitions.Definition, datastore *datastores.Datastore, nestedJobs *[][]jobs.Job) (*results.ResultDefinition, error) {

	// If the definition already has a result in the datastore we download it.
	resultDefinition, err := (*datastore).GetResultDefinition(definition.Id)

	// Otherwise we create an empty one, set all its jobs to waiting and upload it to the datastore before starting the execution.
	switch err.(type) {
	case *errors.ElementNotFoundErr:
		{
			// We inform the user that a new result has been created in the datastore.
			log.Printf(err.Error() + " A new document is created.")

			// Create empty result definition
			resultDefinition = &results.ResultDefinition{
				Id:              definition.Id,
				Name:            definition.Name,
				SpecificationId: definition.SpecificationId,
				ResultJobs:      []results.ResultJob{},
			}

			// We instantiate all its works in a waiting state
			for _, jobGroup := range *nestedJobs {
				for _, job := range jobGroup {
					resultDefinition.ResultJobs = append(resultDefinition.ResultJobs, results.ResultJob{Id: job.Id, Name: job.Name, Status: results.Waiting})
				}
			}

			// We add the result definition to the datastore
			err := (*datastore).AddResultDefinition(resultDefinition)
			if err != nil {
				return nil, fmt.Errorf("error during insertion into the datastore %s", err.Error())
			}
		}
	default:
		// In case of another error, it is returned in the function
		if err != nil {
			return nil, err
		}
	}

	return resultDefinition, nil
}

// executeJobs function is responsible for executing the jobs in the order established in the
// two-dimensional list. In addition, it stores real-time information in the datastore in order
// to facilitate the resolution of cuts during execution. It takes as input the pointer of the
// Jobs set, the pointer of an Executor variable, the pointer of a ResultDefinition variable and
// the pointer of a Datastore variable. It returns the pointer to an array of ResultJob and an
// error variable to report any problems.
func executeJobs(nestedJobs *[][]jobs.Job, executor *executors.Executor, resultDefinition *results.ResultDefinition, datastore *datastores.Datastore) (*[]results.ResultJob, error) {

	// We create a map for storing the results of each job (local storage)
	jobResults := make(map[string]results.ResultJob)

	// We created an access control system to prevent co-occurrence into goroutines
	mutex := &sync.RWMutex{}

	// We fill the map with the input Result Definition (remote storage)
	for _, jobRes := range resultDefinition.ResultJobs {
		jobResults[jobRes.Name] = jobRes
	}

	// For each group of tasks ...
	for _, jobGroup := range *nestedJobs {

		// We create an error group to allow waiting for all tasks belonging to the group and collect any error
		var errg errgroup.Group

		// For each job in the group ...
		for _, job := range jobGroup {

			// Verify that the job is pending execution and that none of its dependencies have been cancelled.
			validForExecution, err := checkJobExecutionRequirements(&job, &jobResults, datastore, resultDefinition.Id)
			if err != nil {
				return nil, err
			}

			// If none of its dependencies have previously failed and the job is pending, it is put into execution in a goroutine.
			/**
			  See https://gobyexample.com/waitgroups,
			  https://go.dev/doc/faq#closures_and_goroutines,
			  https://stackoverflow.com/questions/18499352/golang-concurrency-how-to-append-to-the-same-slice-from-different-goroutines
			*/
			if validForExecution {
				j := job
				errg.Go(func() error {
					return runAndUpdateStatus(executor, &j, mutex, &jobResults, datastore, resultDefinition.Id)
				})
			}
		}

		// Wait for all group tasks to be completed before starting the next group
		err := errg.Wait()
		if err != nil {
			return nil, err
		}
	}

	res := maps.Values(jobResults)
	return &res, nil
}

// checkJobExecutionRequirements function checks that the job is pending execution and
// that none of its dependencies have been cancelled. To do so, it takes as input the
// pointer to a Job variable, the pointer to a ResultJob map, a pointer to a Datastore
// variable and the id of the ResultDefinition. In the output it provides a boolean value
// and an error variable to report any problems.

func checkJobExecutionRequirements(job *jobs.Job, jobResults *map[string]results.ResultJob, datastore *datastores.Datastore, resultDefinitionId string) (bool, error) {

	// If the job is not pending execution, it is ignored.
	pending := (*jobResults)[job.Name].Status == results.Waiting

	// If the job must be cancelled, it is ignored.
	cancelled := false

	// For each dependencie job ...
	for _, depName := range job.Dependencies {

		// If any of its dependencies has previously failed, the job is cancelled and its execution is dispensed with.
		if (*jobResults)[depName].Status == results.Error || (*jobResults)[depName].Status == results.Cancelled {

			// Cancel current job
			cancelled = true

			// Create Result Job
			jobRes := results.ResultJob{Id: job.Id, Name: job.Name, Logs: "Cancelled due to errors in its dependencies", Status: results.Cancelled}

			// Save result job in local storage
			(*jobResults)[job.Name] = jobRes

			// Save result job in remote storage
			err := (*datastore).UpdateResultJob(&jobRes, resultDefinitionId)
			if err != nil {
				return false, err
			}

			// If one of the dependencies has already failed, the search is stopped.
			break
		}
	}

	return pending && !cancelled, nil
}

// runAndUpdateStatus function is responsible for calling the executor to run the job and then
// update its status in the local variable and in the remote datastore. It takes as input the pointer
// of an Executor variable, the pointer to a Job variable, the pointer to a sync.RWMutex variable, the
// pointer to a ResultJob map, a pointer to a Datastore variable and the id of the ResultDefinition.
// In the output it provides an error variable to report any problems.
func runAndUpdateStatus(executor *executors.Executor, job *jobs.Job, mutex *sync.RWMutex, jobResults *map[string]results.ResultJob, datastore *datastores.Datastore, resultDefinitionId string) error {

	// Execute job
	jobRes, err := (*executor).ExecuteJob(job)
	if err != nil {
		return err
	}

	// Save result in local storage (with control access)
	mutex.Lock()
	(*jobResults)[job.Name] = *jobRes
	mutex.Unlock()

	// Save result in remote storage
	updateErr := (*datastore).UpdateResultJob(jobRes, resultDefinitionId)
	if updateErr != nil {
		return updateErr
	}

	return nil
}
