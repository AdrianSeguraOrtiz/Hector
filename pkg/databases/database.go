package databases

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/databases/dbmock"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/results"
	"dag/hector/golang/module/pkg/specifications"
	"fmt"
)

type Database interface {
	GetComponent(id string) (components.Component, error)
	GetSpecification(id string) (specifications.Specification, error)
	GetTopologicalSort(id string) ([][]string, error)
	GetDefinition(id string) (definitions.Definition, error)
	GetResultDefinition(id string) (results.ResultDefinition, error)

	AddComponent(componentPointer *components.Component) error
	AddSpecification(specificationPointer *specifications.Specification) error
	AddTopologicalSort(planning [][]string, specificationId string) error
	AddDefinition(definitionPointer *definitions.Definition) error
	AddResultDefinition(resultDefinitionPointer *results.ResultDefinition) error

	UpdateResultJob(resultJobPointer *results.ResultJob, resultDefinitionId string) error
	GetDefinitionsWithWaitings() ([]definitions.Definition, error)
}

func NewDatabase(repo string) (*Database, error) {
	var database Database

	switch repo {
	case "mock":
		database = dbmock.NewDBMock()
	default:
		return nil, fmt.Errorf("invalid repo: %v", repo)
	}

	return &database, nil
}
