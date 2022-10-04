package schedulers

import (
	"dag/hector/golang/module/pkg/schedulers/topologicalgrouped"
	"dag/hector/golang/module/pkg/specifications"
	"fmt"
)

type Scheduler interface {
	Plan(specification *specifications.Specification) ([][]string, error)
}

func NewScheduler(strategy string) (*Scheduler, error) {
	var scheduler Scheduler

	switch strategy {
	case "topological_grouped":
		scheduler = topologicalgrouped.NewTopologicalGrouped()
	default:
		return nil, fmt.Errorf("invalid strategy: %v", strategy)
	}

	return &scheduler, nil
}
