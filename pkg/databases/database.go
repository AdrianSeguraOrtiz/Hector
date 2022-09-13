package databases

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/workflows"
)

type Database interface {
	GetWorkflow(id string) (workflows.Workflow, error)
	GetTopologicalSort(id string) ([][]string, error)
	GetComponent(id string) (components.Component, error)
}