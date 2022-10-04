package executor

import "hector/pkg/workflow"

type Executor struct {
}

func NewExecutor() *Executor {
	return &Executor{}
}

func (ex *Executor) Run(task *workflow.Task) (TaskResult, error) {
	// TODO - actually run the task

	execResult := TaskResult{
		Status: TaskSkipped,
	}

	return execResult, nil
}
