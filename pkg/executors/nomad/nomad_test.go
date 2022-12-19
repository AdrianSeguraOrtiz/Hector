package nomad

import (
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/filemanagers"
	"dag/hector/golang/module/pkg/filemanagers/minio"
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/nomad/api"
)

func TestArgumentsToSlice(t *testing.T) {
	var tests = []struct {
		arguments *[]definitions.Parameter
		array     []string
	}{
		{
			arguments: &[]definitions.Parameter{
				{
					Name:  "name-1",
					Value: "value-1",
				},
			},
			array: []string{"--name-1", "value-1"},
		},
		{
			arguments: &[]definitions.Parameter{
				{
					Name:  "name-2",
					Value: 2,
				},
			},
			array: []string{"--name-2", "2"},
		},
		{
			arguments: &[]definitions.Parameter{
				{
					Name:  "name-3",
					Value: true,
				},
			},
			array: []string{"--name-3", "true"},
		},
		{
			arguments: &[]definitions.Parameter{
				{
					Name:  "name-1",
					Value: "value-1",
				},
				{
					Name:  "name-2",
					Value: 2,
				},
				{
					Name:  "name-3",
					Value: true,
				},
			},
			array: []string{"--name-1", "value-1", "--name-2", "2", "--name-3", "true"},
		},
	}

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			args := argumentsToSlice(tt.arguments)
			if !reflect.DeepEqual(args, tt.array) {
				t.Error("got ", args, ", want ", tt.array)
			}
		})
	}

}

func TestBuildJob(t *testing.T) {
	var tests = []struct {
		job      *jobs.Job
		nomadJob *api.Job
	}{
		{
			job: &jobs.Job{
				Id:    "Job-Id-1",
				Name:  "Job Name 1",
				Image: "image-name-1",
			},
			nomadJob: &api.Job{
				ID:          pkg.Ptr("Job-Id-1"),
				Name:        pkg.Ptr("Job Name 1"),
				Type:        pkg.Ptr("batch"),
				Datacenters: []string{"dc1"},
				TaskGroups: []*api.TaskGroup{
					{
						Name: pkg.Ptr("Task-Group-Job-Id-1"),
						Tasks: []*api.Task{
							{
								Name:   "Task-Job-Id-1",
								Driver: "docker",
								Config: map[string]interface{}{
									"image": "image-name-1",
									"args":  []string{},
								},
								RestartPolicy: &api.RestartPolicy{Attempts: pkg.Ptr(0)},
							},
						},
						RestartPolicy: &api.RestartPolicy{Attempts: pkg.Ptr(0)},
					},
				},
				Reschedule: &api.ReschedulePolicy{Attempts: pkg.Ptr(0)},
			},
		},
		{
			job: &jobs.Job{
				Id:    "Job-Id-2",
				Name:  "Job Name 2",
				Image: "image-name-2",
				Arguments: []definitions.Parameter{
					{
						Name:  "name-1",
						Value: "value-1",
					},
					{
						Name:  "name-2",
						Value: 2,
					},
					{
						Name:  "name-3",
						Value: true,
					},
				},
			},
			nomadJob: &api.Job{
				ID:          pkg.Ptr("Job-Id-2"),
				Name:        pkg.Ptr("Job Name 2"),
				Type:        pkg.Ptr("batch"),
				Datacenters: []string{"dc1"},
				TaskGroups: []*api.TaskGroup{
					{
						Name: pkg.Ptr("Task-Group-Job-Id-2"),
						Tasks: []*api.Task{
							{
								Name:   "Task-Job-Id-2",
								Driver: "docker",
								Config: map[string]interface{}{
									"image": "image-name-2",
									"args":  []string{"--name-1", "value-1", "--name-2", "2", "--name-3", "true"},
								},
								RestartPolicy: &api.RestartPolicy{Attempts: pkg.Ptr(0)},
							},
						},
						RestartPolicy: &api.RestartPolicy{Attempts: pkg.Ptr(0)},
					},
				},
				Reschedule: &api.ReschedulePolicy{Attempts: pkg.Ptr(0)},
			},
		},
	}

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			taskName := "Task-" + tt.job.Id
			taskGroupName := "Task-Group-" + tt.job.Id
			nomadJob := buildJob(tt.job, taskName, taskGroupName)

			if equal, message := pkg.DeepValueEqual(*nomadJob, *tt.nomadJob, true); !equal {
				t.Error(message)
			}
		})
	}
}

func TestWaitForJob(t *testing.T) {
	var tests = []struct {
		jobId          string
		statusSequence []results.Status
		expectedStatus results.Status
		expectedTime   int64
		err            bool
	}{
		{
			jobId:          "Job-Id-1",
			statusSequence: []results.Status{results.Done},
			expectedStatus: results.Done,
			expectedTime:   10,
			err:            false,
		},
		{
			jobId:          "Job-Id-2",
			statusSequence: []results.Status{results.Error},
			expectedStatus: results.Error,
			expectedTime:   10,
			err:            false,
		},
		{
			jobId:          "Job-Id-3",
			statusSequence: []results.Status{results.Waiting, results.Done},
			expectedStatus: results.Done,
			expectedTime:   20,
			err:            false,
		},
		{
			jobId:          "Job-Id-4",
			statusSequence: []results.Status{results.Waiting, results.Waiting, results.Error},
			expectedStatus: results.Error,
			expectedTime:   30,
			err:            false,
		},
		{
			jobId:          "Job-Id-5",
			statusSequence: []results.Status{results.Waiting, results.Done},
			expectedStatus: results.Waiting,
			expectedTime:   10,
			err:            true,
		},
	}

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		cnt := 0
		taskGroupName := "Task-Group-" + tt.jobId

		getSummaryMock := func(jobId string, qo *api.QueryOptions) (*api.JobSummary, *api.QueryMeta, error) {
			summary := make(map[string]api.TaskGroupSummary, 1)

			switch tt.statusSequence[cnt] {
			case results.Done:
				summary[taskGroupName] = api.TaskGroupSummary{Complete: 1}
			case results.Error:
				summary[taskGroupName] = api.TaskGroupSummary{Failed: 1}
			default:
				summary[taskGroupName] = api.TaskGroupSummary{Running: 1}
			}

			cnt += 1
			if tt.err {
				return nil, nil, fmt.Errorf("the task status could not be detected")
			}
			return &api.JobSummary{Summary: summary}, nil, nil
		}

		t.Run(testname, func(t *testing.T) {
			start := time.Now()
			status, err := waitForJob(tt.jobId, taskGroupName, getSummaryMock)
			end := time.Now()

			var errMsg string
			if err != nil {
				errMsg = err.Error()
			}

			if (tt.err && errMsg != "the task status could not be detected") || (!tt.err && err != nil) {
				t.Error("The error produced by the function has not been as expected")
			}
			if status != tt.expectedStatus {
				t.Error("The execution status is not as expected. Wanted " + fmt.Sprintf("%v", tt.expectedStatus) + " got " + fmt.Sprintf("%v", status))
			}
			if runTime := end.Sub(start).Milliseconds(); math.Abs(float64(runTime-tt.expectedTime)) > 2 {
				t.Error("The waiting time has been different than expected. Wanted " + fmt.Sprintf("%v", tt.expectedTime) + "ms got " + fmt.Sprintf("%v", runTime) + "ms")
			}
		})
	}
}

func TestGetAllocation(t *testing.T) {
	type errs struct {
		getAllAllocations bool
		numAllocations    bool
		getAllocInfo      bool
	}
	var tests = []struct {
		jobId string
		alloc *api.Allocation
		errs  errs
	}{
		{
			jobId: "Job-Id-1",
			alloc: &api.Allocation{JobID: "Job-Id-1"},
			errs: errs{
				getAllAllocations: false,
				numAllocations:    false,
				getAllocInfo:      false,
			},
		},
		{
			jobId: "Job-Id-2",
			alloc: nil,
			errs: errs{
				getAllAllocations: true,
				numAllocations:    false,
				getAllocInfo:      false,
			},
		},
		{
			jobId: "Job-Id-3",
			alloc: nil,
			errs: errs{
				getAllAllocations: false,
				numAllocations:    true,
				getAllocInfo:      false,
			},
		},
		{
			jobId: "Job-Id-4",
			alloc: nil,
			errs: errs{
				getAllAllocations: false,
				numAllocations:    false,
				getAllocInfo:      true,
			},
		},
	}

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		getAllAllocationsMock := func(jobID string, allAllocs bool, q *api.QueryOptions) ([]*api.AllocationListStub, *api.QueryMeta, error) {
			if tt.errs.getAllAllocations {
				return nil, nil, fmt.Errorf("it has not been possible to extract the allocations")
			} else if tt.errs.numAllocations {
				return []*api.AllocationListStub{{JobID: tt.jobId}, {}}, nil, nil
			}
			return []*api.AllocationListStub{{JobID: tt.jobId}}, nil, nil
		}

		getAllocInfoMock := func(allocID string, q *api.QueryOptions) (*api.Allocation, *api.QueryMeta, error) {
			if tt.errs.getAllocInfo {
				return nil, nil, fmt.Errorf("it has not been possible to extract alloc info")
			}
			return &api.Allocation{JobID: tt.jobId}, nil, nil
		}

		t.Run(testname, func(t *testing.T) {
			alloc, err := getAllocation(tt.jobId, getAllAllocationsMock, getAllocInfoMock)
			var errMsg string
			if err != nil {
				errMsg = err.Error()
			}

			if (tt.errs.getAllAllocations && errMsg != "it has not been possible to extract the allocations") ||
				(tt.errs.numAllocations && errMsg != "unexpected number of allocs: 2") ||
				(tt.errs.getAllocInfo && errMsg != "it has not been possible to extract alloc info") ||
				(!tt.errs.getAllAllocations && !tt.errs.numAllocations && !tt.errs.getAllocInfo && err != nil) {
				t.Error("The error produced by the function has not been as expected")
			}

			if alloc == nil || tt.alloc == nil {
				if alloc != tt.alloc {
					t.Error("The allocation obtained is not as expected. Want " + fmt.Sprintf("%v", tt.alloc) + " got" + fmt.Sprintf("%v", alloc))
				}
			} else if equal, msg := pkg.DeepValueEqual(*alloc, *tt.alloc, true); !equal {
				t.Error("The allocation obtained is not as expected. " + msg)
			}
		})
	}
}

func TestGetLogsFromAllocation(t *testing.T) {
	var tests = []struct {
		alloc       *api.Allocation
		status      results.Status
		expectedLog string
		err         bool
	}{
		{
			alloc:       &api.Allocation{JobID: "Job-Id-1"},
			status:      results.Done,
			expectedLog: "All right",
			err:         false,
		},
		{
			alloc:       &api.Allocation{JobID: "Job-Id-2"},
			status:      results.Error,
			expectedLog: "A task error has occurred",
			err:         false,
		},
		{
			alloc:       &api.Allocation{JobID: "Job-Id-3"},
			status:      results.Done,
			expectedLog: "",
			err:         true,
		},
	}

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		getChannelLogsMock := func(alloc *api.Allocation, follow bool, task string, logType string, origin string, offset int64, cancel <-chan struct{}, q *api.QueryOptions) (<-chan *api.StreamFrame, <-chan error) {

			frames := make(chan *api.StreamFrame, 1)
			var log string
			switch logType {
			case "stdout":
				log = "All right"
			case "stderr":
				log = "A task error has occurred"
			}
			frames <- &api.StreamFrame{Data: []byte(log)}

			errors := make(chan error, 1)
			if tt.err {
				errors <- fmt.Errorf("Logs could not be extracted")
			}

			return frames, errors
		}

		t.Run(testname, func(t *testing.T) {
			logs, err := getLogsFromAllocation(tt.alloc, tt.status, "Task-"+tt.alloc.JobID, getChannelLogsMock)
			var errMsg string
			if err != nil {
				errMsg = err.Error()
			}

			if (tt.err && errMsg != "Logs could not be extracted") || (!tt.err && err != nil) {
				t.Error("The error produced by the function has not been as expected")
			}
			if logs != tt.expectedLog {
				t.Error("Logs obtained are not as expected. Wanted " + tt.expectedLog + " got " + logs)
			}
		})
	}
}

func TestExecuteJob(t *testing.T) {
	var tests = []struct {
		job    *jobs.Job
		logs   string
		status results.Status
	}{
		{
			job: &jobs.Job{
				Id:    "Job-Id-1",
				Name:  "Job Name 1",
				Image: "hello-world",
			},
			logs:   "Hello from Docker!",
			status: results.Done,
		},

		{
			job: &jobs.Job{
				Id:    "Job-Id-2",
				Name:  "Job Name 2",
				Image: "bad-image-name",
			},
			logs:   "pull access denied for bad-image-name, repository does not exist or may require 'docker login': denied: requested access to the resource is denied",
			status: results.Error,
		},

		{
			job: &jobs.Job{
				Id:    "Job-Id-3",
				Name:  "Job Name 3",
				Image: "adriansegura99/dag_kubernetes_concat-messages",
				Arguments: []definitions.Parameter{
					{
						Name:  "message-1",
						Value: "The best",
					},
					{
						Name:  "message-2",
						Value: "test",
					},
				},
			},
			logs:   "The best test",
			status: results.Done,
		},

		{
			job: &jobs.Job{
				Id:    "Job-Id-4",
				Name:  "Job Name 4",
				Image: "adriansegura99/dag_kubernetes_concat-messages",
				Arguments: []definitions.Parameter{
					{
						Name:  "bad-name",
						Value: "Bad Value",
					},
				},
			},
			logs:   "No such option: --bad-name",
			status: results.Error,
		},
	}

	// Create FileManager
	var fileManager filemanagers.FileManager
	fileManager, _ = minio.NewMinio()

	// Create Nomad
	nomad := NewNomad(&fileManager)

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			resJob, err := nomad.ExecuteJob(tt.job)

			if err != nil {
				t.Error(err)
			} else if resJob.Status != tt.status {
				t.Error("Invalid status, got ", resJob.Status, ", want ", tt.status)
			} else if !strings.Contains(resJob.Logs, tt.logs) {
				t.Error("Invalid logs, got ", resJob.Logs, ", want something like ", tt.logs)
			}
		})
	}

	defer nomad.Client.System().GarbageCollect()
}
