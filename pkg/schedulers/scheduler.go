package schedulers

import (
	"dag/hector/golang/module/pkg/specifications"
)

type Scheduler interface {
	Plan(specification *specifications.Specification) ([][]string, error)
}
