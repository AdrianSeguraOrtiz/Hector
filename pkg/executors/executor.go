package executors

import (
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
)

type Executor interface {
	ExecuteJob(jobPointer *jobs.Job) (*results.ResultJob, error)
}
