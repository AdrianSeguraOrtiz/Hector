package nomad

import (
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"fmt"
	"time"

	"github.com/hashicorp/nomad/api"
	"golang.org/x/exp/slices"
)

func argumentsToSlice(arguments *[]definitions.Parameter) []string {
	var args []string
	for _, arg := range *arguments {
		args = append(args, "--"+arg.Name)
		args = append(args, arg.Value.(string))
	}
	return args
}

func buildJob(job *jobs.Job, taskName string, taskGroupName string) *api.Job {
	/**
	This function is responsible for constructing the definition of a
	nomad's own task from the Hector's own task pointer.
	*/

	// 1. Task
	args := argumentsToSlice(&job.Arguments)
	attempts := 0
	nomadTask := &api.Task{
		Name:   taskName,
		Driver: "docker",
		Config: map[string]interface{}{
			"image": job.Image,
			"args":  args,
		},
		RestartPolicy: &api.RestartPolicy{Attempts: &attempts},
	}

	// 2. Task Group
	nomadTaskGroup := &api.TaskGroup{
		Name:          &taskGroupName,
		Tasks:         []*api.Task{nomadTask},
		RestartPolicy: &api.RestartPolicy{Attempts: &attempts},
	}

	// 3. Job
	jobType := "batch"
	nomadJob := &api.Job{
		ID:          &job.Id,
		Name:        &job.Name,
		Type:        &jobType,
		Datacenters: []string{"dc1"},
		TaskGroups:  []*api.TaskGroup{nomadTaskGroup},
		Reschedule:  &api.ReschedulePolicy{Attempts: &attempts},
	}

	return nomadJob
}

type Nomad struct {
	Client *api.Client
}

func NewNomad() *Nomad {
	cfg := api.DefaultConfig()
	client, _ := api.NewClient(cfg)
	return &Nomad{Client: client}
}

func (no *Nomad) ExecuteJob(job *jobs.Job) (*results.ResultJob, error) {

	// We print the initialization message and display the job information
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
	status, err := no.waitForJob(job.Id, taskGroupName)
	if err != nil {
		return nil, err
	}

	// We print the finalization message
	fmt.Println("Finished " + job.Name + " job\n")

	// Get allocation of our executed job
	alloc, err := no.getAllocation(job.Id)
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
	logs, err := no.getLogsFromAllocation(alloc, status, taskName)
	if err != nil {
		return nil, err
	}

	// We return the result job
	return &results.ResultJob{Id: job.Id, Name: job.Name, Logs: warnings + logs, Status: status}, nil
}

func (no *Nomad) waitForJob(jobId string, taskGroupName string) (results.Status, error) {
	/**
	This function is in charge of waiting for the execution
	of the job whose id is provided as input parameter

	NOTE: https://github.com/hashicorp/nomad/issues/6818
	*/

	status := results.Waiting
	for status == results.Waiting {

		// We establish pauses of 10 milliseconds
		time.Sleep(10 * time.Millisecond)

		// We obtain the most summarized information of our job (minimum amount of information found so as not to overload the loop)
		jobSummary, _, err := no.Client.Jobs().Summary(jobId, nil)
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

func (no *Nomad) getAllocation(jobId string) (*api.Allocation, error) {
	/**
	This function returns the allocation corresponding to the previously
	executed job whose id is provided as the input parameter
	*/

	// Get the list of allocations generated by our job
	allocs, _, err := no.Client.Jobs().Allocations(jobId, true, nil)
	if err != nil {
		return nil, err
	}

	// Since the constructed job has only one task group, we limit the number of allocations to 1
	if len(allocs) != 1 {
		return nil, fmt.Errorf("unexpected number of allocs: %d", len(allocs))
	}

	// We obtain the information of the allocation found
	alloc, _, err := no.Client.Allocations().Info(allocs[0].ID, nil)
	if err != nil {
		return nil, err
	}

	return alloc, nil
}

func (no *Nomad) getLogsFromAllocation(alloc *api.Allocation, status results.Status, taskName string) (string, error) {
	/**
	This function is responsible for extracting the
	logs stored in the allocation of our job.
	*/

	// Select the reading channel according to the task status
	var channel string
	if status == results.Done {
		channel = "stdout"
	} else {
		channel = "stderr"
	}

	// Get logs from our allocation
	cancel := make(chan struct{})
	frames, errors := no.Client.AllocFS().Logs(alloc, false, taskName, channel, "start", 0, cancel, nil)

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