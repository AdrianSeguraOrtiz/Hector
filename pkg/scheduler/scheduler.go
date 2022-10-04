package scheduler

import "hector/pkg/workflow"

type Scheduler struct {
	Strategy string
}

func NewScheduler() *Scheduler {
	return &Scheduler{Strategy: "topological"}
}

func (sc *Scheduler) Plan(wf *workflow.WorkflowSpec) ([][]string, error) {
	planning, err := workflow.TopologicalGroupedSort(wf)
	if err != nil {
		return nil, err
	}
	return planning, nil
}
