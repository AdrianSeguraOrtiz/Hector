package nomad

import (
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/filemanagers"
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/nomad/api"
	"golang.org/x/exp/slices"
)

type Nomad struct {
	Client      *api.Client
	FileManager *filemanagers.FileManager
}

func NewNomad(fileManager *filemanagers.FileManager) *Nomad {
	cfg := api.DefaultConfig()
	client, _ := api.NewClient(cfg)
	return &Nomad{Client: client, FileManager: fileManager}
}

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
	// 1. Download sidecar
	downLogs, err := getLogsFromAllocation(alloc, status, "download-task", no.Client.AllocFS().Logs)
	if err != nil {
		return nil, err
	}
	// 2. Main task
	mainLogs, err := getLogsFromAllocation(alloc, status, taskName, no.Client.AllocFS().Logs)
	if err != nil {
		return nil, err
	}
	// 3. Upload sidecar
	upLogs, err := getLogsFromAllocation(alloc, status, "upload-task", no.Client.AllocFS().Logs)
	if err != nil {
		return nil, err
	}
	// 4. Join logs
	logs := downLogs + mainLogs + upLogs

	// We return the result job
	return &results.ResultJob{Id: job.Id, Name: job.Name, Logs: warnings + logs, Status: status}, nil
}

func argumentsToSlice(arguments *[]definitions.Parameter) []string {
	/**
	This function takes Hector's own parameter definitions and converts
	them into an array of strings by adding dashes to the tags
	*/

	var args []string
	for _, arg := range *arguments {
		args = append(args, "--"+arg.Name)
		args = append(args, fmt.Sprintf("%v", arg.Value))
	}
	return args
}

func buildJob(job *jobs.Job, taskName string, taskGroupName string) *api.Job {
	/**
	This function is responsible for constructing the definition of a
	nomad's own task from the Hector's own task pointer.
	*/

	// 1. Main Task
	args := argumentsToSlice(&job.Arguments)
	nomadTask := &api.Task{
		Name:   taskName,
		Driver: "docker",
		Config: map[string]interface{}{
			"image":   job.Image,
			"args":    args,
			"volumes": []string{"../alloc:/usr/local/src/data"},
		},
		RestartPolicy: &api.RestartPolicy{Attempts: pkg.Ptr(0)},
	}

	// 2. Sidecar (Download and Upload)
	filemanagerImage := "adriansegura99/hector_sidecar:1.0.0"
	envVars := map[string]string{
		"MINIO_ENDPOINT":          os.Getenv("MINIO_ENDPOINT"),
		"MINIO_ACCESS_KEY_ID":     os.Getenv("MINIO_ACCESS_KEY_ID"),
		"MINIO_SECRET_ACCESS_KEY": os.Getenv("MINIO_SECRET_ACCESS_KEY"),
		"MINIO_USE_SSL":           os.Getenv("MINIO_USE_SSL"),
		"MINIO_BUCKET_NAME":       os.Getenv("MINIO_BUCKET_NAME"),
	}

	var downloadPaths []string
	for _, path := range job.RequiredFiles {
		downloadPaths = append(downloadPaths, "--local-path", "data/"+filepath.Base(path), "--remote-path", path)
	}
	downloadTask := &api.Task{
		Name: "download-task",
		Lifecycle: &api.TaskLifecycle{
			Hook:    api.TaskLifecycleHookPrestart,
			Sidecar: false,
		},
		Driver: "docker",
		Config: map[string]interface{}{
			"image":   filemanagerImage,
			"args":    append([]string{"download"}, downloadPaths...),
			"volumes": []string{"../alloc:/usr/local/src/data"},
		},
		Env:           envVars,
		RestartPolicy: &api.RestartPolicy{Attempts: pkg.Ptr(0)},
	}

	var uploadPaths []string
	for _, path := range job.OutputFiles {
		uploadPaths = append(uploadPaths, "--local-path", "data/"+filepath.Base(path), "--remote-path", path)
	}
	uploadTask := &api.Task{
		Name: "upload-task",
		Lifecycle: &api.TaskLifecycle{
			Hook:    api.TaskLifecycleHookPoststop,
			Sidecar: false,
		},
		Driver: "docker",
		Config: map[string]interface{}{
			"image":   filemanagerImage,
			"args":    append([]string{"upload"}, uploadPaths...),
			"volumes": []string{"../alloc:/usr/local/src/data"},
		},
		Env:           envVars,
		RestartPolicy: &api.RestartPolicy{Attempts: pkg.Ptr(0)},
	}

	// 3. Task Group
	nomadTaskGroup := &api.TaskGroup{
		Name:          &taskGroupName,
		Tasks:         []*api.Task{downloadTask, nomadTask, uploadTask},
		RestartPolicy: &api.RestartPolicy{Attempts: pkg.Ptr(0)},
	}

	// 4. Job
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

func waitForJob(jobId string, taskGroupName string, getSummary func(string, *api.QueryOptions) (*api.JobSummary, *api.QueryMeta, error)) (results.Status, error) {
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

func getAllocation(jobId string, getAllAllocations func(string, bool, *api.QueryOptions) ([]*api.AllocationListStub, *api.QueryMeta, error), getAllocInfo func(string, *api.QueryOptions) (*api.Allocation, *api.QueryMeta, error)) (*api.Allocation, error) {
	/**
	This function returns the allocation corresponding to the previously
	executed job whose id is provided as the input parameter
	*/

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

func getLogsFromAllocation(alloc *api.Allocation, status results.Status, taskName string, getChannelLogs func(*api.Allocation, bool, string, string, string, int64, <-chan struct{}, *api.QueryOptions) (<-chan *api.StreamFrame, <-chan error)) (string, error) {
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
		if frame != nil {
			logs += string(frame.Data)
		}
	}

	return logs, nil
}
