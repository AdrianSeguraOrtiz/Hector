package databases

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/databases/dbmock"
	"dag/hector/golang/module/pkg/databases/sqlite3"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/results"
	"dag/hector/golang/module/pkg/specifications"
)

type Database interface {
	GetComponent(id string) (*components.Component, error)
	GetSpecification(id string) (*specifications.Specification, error)
	GetPlanning(id string) (*[][]string, error)
	GetDefinition(id string) (*definitions.Definition, error)
	GetResultDefinition(id string) (*results.ResultDefinition, error)

	AddComponent(component *components.Component) error
	AddSpecification(specification *specifications.Specification) error
	AddPlanning(planning *[][]string, specificationId string) error
	AddDefinition(definition *definitions.Definition) error
	AddResultDefinition(resultDefinition *results.ResultDefinition) error

	UpdateResultJob(resultJob *results.ResultJob, resultDefinitionId string) error
	GetDefinitionsWithWaitings() (*[]definitions.Definition, error)
}

func NewDatabase(repo string) (*Database, error) {
	var database Database
	var err error

	switch repo {
	case "mock":
		database = dbmock.NewDBMock()
	case "sqlite3":
		database, err = sqlite3.NewSQLite3()
	default:
		return nil, fmt.Errorf("invalid repo: %v", repo)
	}

	return &database, err
}
