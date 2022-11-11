package nomad

import (
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/definitions"
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
		expectedTime   int64
	}{
		{
			jobId:          "Job-Id-1",
			statusSequence: []results.Status{results.Done},
			expectedTime:   10,
		},
		{
			jobId:          "Job-Id-2",
			statusSequence: []results.Status{results.Error},
			expectedTime:   10,
		},
		{
			jobId:          "Job-Id-3",
			statusSequence: []results.Status{results.Waiting, results.Done},
			expectedTime:   20,
		},
		{
			jobId:          "Job-Id-4",
			statusSequence: []results.Status{results.Waiting, results.Waiting, results.Error},
			expectedTime:   30,
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
			return &api.JobSummary{Summary: summary}, nil, nil
		}

		t.Run(testname, func(t *testing.T) {
			start := time.Now()
			status, _ := waitForJob(tt.jobId, taskGroupName, getSummaryMock)
			end := time.Now()

			if expectedStatus := tt.statusSequence[len(tt.statusSequence)-1]; status != expectedStatus {
				t.Error("The execution status is not as expected. Wanted " + fmt.Sprintf("%v", expectedStatus) + " got " + fmt.Sprintf("%v", status))
			}
			if runTime := end.Sub(start).Milliseconds(); math.Abs(float64(runTime-tt.expectedTime)) > 2 {
				t.Error("The waiting time has been different than expected. Wanted " + fmt.Sprintf("%v", tt.expectedTime) + "ms got " + fmt.Sprintf("%v", runTime) + "ms")
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

	nomad := NewNomad()

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
}
