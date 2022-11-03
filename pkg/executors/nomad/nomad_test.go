package nomad

import (
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"strconv"
	"strings"
	"testing"
)

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
			resJobPointer, err := nomad.ExecuteJob(tt.job)

			if err != nil {
				t.Error(err)
			} else if (*resJobPointer).Status != tt.status {
				t.Error("Invalid status, got ", (*resJobPointer).Status, ", want ", tt.status)
			} else if !strings.Contains((*resJobPointer).Logs, tt.logs) {
				t.Error("Invalid logs, got ", (*resJobPointer).Logs, ", want something like ", tt.logs)
			}
		})
	}
}
