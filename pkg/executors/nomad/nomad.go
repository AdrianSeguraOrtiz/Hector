package nomad

import (
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"fmt"
	"time"

	"github.com/hashicorp/nomad/api"
	"golang.org/x/exp/slices"
)

type Nomad struct {
	Client *api.Client
}

// NewNomad function creates a new instance of the Nomad type. It returns a pointer to the
// constructed variable.
func NewNomad() *Nomad {
	cfg := api.DefaultConfig()
	client, _ := api.NewClient(cfg)
	return &Nomad{Client: client}
}

// ExecuteJob function is responsible for the execution of a Job. It takes as input the
// pointer of a given Job. It provides as output a pointer to the generated ResultJob and
// an error variable in charge of notifying any problem.
func (no *Nomad) ExecuteJob(job *jobs.Job) (*results.ResultJob, error) {

	// TODO: Replace fmt.Prints with loggers
	// We print the initialization message
	fmt.Printf("Started "+job.Name+" job. Info: \n\t %+v\n\n", *job)

	// Build nomad job from our pointer
	taskName := "Task-" + job.Id
	taskGroupName := "Task-Group-" + job.Id
	nomadJob := buildJob(job, taskName, taskGroupName)

	// We start to execute the job
	jobRegisterResponse, _, err := no.Client.Jobs().Register(nomadJob, nil)
	if err != nil {
		return nil, err
	}

	// If there are any warnings, they will be stored in logs
	warnings := jobRegisterResponse.Warnings

	// Delete job after function execution
	defer no.Client.Jobs().Deregister(job.Id, true, nil)

	// We wait for the execution to finish
	status, err := waitForJob(job.Id, taskGroupName, no.Client.Jobs().Summary)
	if err != nil {
		return nil, err
	}

	// TODO: Replace fmt.Prints with loggers
	// We print the finalization message
	fmt.Println("Finished " + job.Name + " job\n")

	// Obtain the allocation of our job in order to later access information about its execution.
	alloc, err := getAllocation(job.Id, no.Client.Jobs().Allocations, no.Client.Allocations().Info)
	if err != nil {
		return nil, err
	}

	// If the status of the task is Error then we look for errors caused by docker in loading the image. In that case we return the result job without scanning the task logs.
	if status == results.Error {
		idxFailure := slices.IndexFunc(alloc.TaskStates[taskName].Events, func(event *api.TaskEvent) bool { return event.Type == "Driver Failure" })
		if idxFailure != -1 {
			return &results.ResultJob{Id: job.Id, Name: job.Name, Logs: alloc.TaskStates[taskName].Events[idxFailure].DisplayMessage, Status: status}, nil
		}
	}

	// Get logs from our allocation
	logs, err := getLogsFromAllocation(alloc, status, taskName, no.Client.AllocFS().Logs)
	if err != nil {
		return nil, err
	}

	// We return the result job
	return &results.ResultJob{Id: job.Id, Name: job.Name, Logs: warnings + logs, Status: status}, nil
}

// argumentsToSlice function takes Hector's own parameter definitions and converts
// them into an array of strings by adding dashes to the tags.
func argumentsToSlice(arguments *[]definitions.Parameter) []string {

	var args []string
	for _, arg := range *arguments {
		args = append(args, "--"+arg.Name)
		args = append(args, fmt.Sprintf("%v", arg.Value))
	}
	return args
}

// buildJob function is responsible for constructing the definition of a
// nomad's own task from the Hector's own task pointer. It takes as input
// the pointer of a Hector Job, the name of the task and the name of the
// task group. Returns the pointer to the constructed nomad Job.
func buildJob(job *jobs.Job, taskName string, taskGroupName string) *api.Job {

	// 1. Task
	args := argumentsToSlice(&job.Arguments)
	nomadTask := &api.Task{
		Name:   taskName,
		Driver: "docker",
		Config: map[string]interface{}{
			"image": job.Image,
			"args":  args,
		},
		RestartPolicy: &api.RestartPolicy{Attempts: pkg.Ptr(0)},
	}

	// 2. Task Group
	nomadTaskGroup := &api.TaskGroup{
		Name:          &taskGroupName,
		Tasks:         []*api.Task{nomadTask},
		RestartPolicy: &api.RestartPolicy{Attempts: pkg.Ptr(0)},
	}

	// 3. Job
	nomadJob := &api.Job{
		ID:          &job.Id,
		Name:        &job.Name,
		Type:        pkg.Ptr("batch"),
		Datacenters: []string{"dc1"},
		TaskGroups:  []*api.TaskGroup{nomadTaskGroup},
		Reschedule:  &api.ReschedulePolicy{Attempts: pkg.Ptr(0)},
	}

	return nomadJob
}

// waitForJob function is in charge of waiting for the execution
// of the job whose id is provided as input parameter. It takes
// as input the job identifier, the name of the task group and
// the function in charge of extracting the status of the job
// during its execution in nomad. Returns the final status of
// the job and an error variable to report any problems.
//
// NOTE: https://github.com/hashicorp/nomad/issues/6818
func waitForJob(jobId string, taskGroupName string, getSummary func(string, *api.QueryOptions) (*api.JobSummary, *api.QueryMeta, error)) (results.Status, error) {

	status := results.Waiting
	for status == results.Waiting {

		// We establish pauses of 10 milliseconds
		time.Sleep(10 * time.Millisecond)

		// We obtain the most summarized information of our job (minimum amount of information found so as not to overload the loop)
		jobSummary, _, err := getSummary(jobId, nil)
		if err != nil {
			return status, err
		}

		// If our task group has finished, we change status
		if jobSummary.Summary[taskGroupName].Complete == 1 {
			status = results.Done
		} else if jobSummary.Summary[taskGroupName].Failed == 1 {
			status = results.Error
		}
	}

	return status, nil
}

// getAllocation function returns the allocation corresponding to a specific
// executed job whose id is provided as the input parameter. It takes as input
// the job identifier, the function in charge of obtaining all allocations and
// the function designed to extract information from a given allocation. Returns
// the pointer to the allocation obtained.
func getAllocation(jobId string, getAllAllocations func(string, bool, *api.QueryOptions) ([]*api.AllocationListStub, *api.QueryMeta, error), getAllocInfo func(string, *api.QueryOptions) (*api.Allocation, *api.QueryMeta, error)) (*api.Allocation, error) {

	// Get the list of allocations generated by our job
	allocs, _, err := getAllAllocations(jobId, true, nil)
	if err != nil {
		return nil, err
	}

	// Since the constructed job has only one task group, we limit the number of allocations to 1
	if len(allocs) != 1 {
		return nil, fmt.Errorf("unexpected number of allocs: %d", len(allocs))
	}

	// We obtain the information of the allocation found
	alloc, _, err := getAllocInfo(allocs[0].ID, nil)
	if err != nil {
		return nil, err
	}

	return alloc, nil
}

// getLogsFromAllocation function is responsible for extracting the logs stored
// in the allocation of a job. It takes as input a pointer to the allocation,
// the final status of the task, the name of the task and the function in charge
// of communicating with nomad to extract the logs. Returns a string and an error
// variable to report any problems.
func getLogsFromAllocation(alloc *api.Allocation, status results.Status, taskName string, getChannelLogs func(*api.Allocation, bool, string, string, string, int64, <-chan struct{}, *api.QueryOptions) (<-chan *api.StreamFrame, <-chan error)) (string, error) {

	// Select the reading channel according to the task status
	var channel string
	if status == results.Done {
		channel = "stdout"
	} else {
		channel = "stderr"
	}

	// Get logs from our allocation
	cancel := make(chan struct{})
	frames, errors := getChannelLogs(alloc, false, taskName, channel, "start", 0, cancel, nil)

	// Extract information from channels
	var logs string
	select {

	// We will notify any error
	case logErr := <-errors:
		if logErr != nil {
			return "", logErr
		}

	// Extract the contents of the logs
	case frame := <-frames:
		logs += string(frame.Data)
	}

	return logs, nil
}
