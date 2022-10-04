package executors

import (
	"dag/hector/golang/module/pkg/executors/execgolang"
	"dag/hector/golang/module/pkg/executors/execmock"
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"fmt"
)

type Executor interface {
	ExecuteJob(jobPointer *jobs.Job) (results.ResultJob, error)
}

func NewExecutor(tool string) (*Executor, error) {
	var executor Executor

	switch tool {
	case "mock":
		executor = execmock.NewExecMock()
	case "golang":
		executor = execgolang.NewExecGolang()
	default:
		return nil, fmt.Errorf("invalid tool: %v", tool)
	}

	return &executor, nil
}
