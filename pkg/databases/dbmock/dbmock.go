package dbmock

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/errors"
	"dag/hector/golang/module/pkg/results"
	"dag/hector/golang/module/pkg/specifications"
	"fmt"

	"golang.org/x/exp/slices"
)

// We create a struct type to store the information that should be contained in the supposed database
type DBMock struct {
	ComponentStructs         []components.Component
	SpecificationStructs     []specifications.Specification
	PlanningOfSpecifications map[string][][]string
	DefinitionStructs        []definitions.Definition
	ResultDefinitionStructs  []results.ResultDefinition
}

/**
NewDBMock function creates a new instance of the DBMock type. It returns the pointer
to the constructed variable.
*/
func NewDBMock() *DBMock {
	db := DBMock{}
	db.PlanningOfSpecifications = make(map[string][][]string)
	return &db
}

/**
GetComponent function extracts a concrete Component given its id. It takes as input
the identifier of the Component. It returns the pointer of the Component extracted
from the database and an error variable in charge of notifying any problem.
*/
func (dbm *DBMock) GetComponent(id string) (*components.Component, error) {

	idx := slices.IndexFunc(dbm.ComponentStructs, func(c components.Component) bool { return c.Id == id })
	if idx == -1 {
		return nil, &errors.ElementNotFoundErr{Type: "components.Component", Id: id}
	}
	component := dbm.ComponentStructs[idx]
	return &component, nil
}

/**
GetSpecification function extracts a concrete Specification given its id. It takes as input
the identifier of the Specification. It returns the pointer of the Specification extracted
from the database and an error variable in charge of notifying any problem.
*/
func (dbm *DBMock) GetSpecification(id string) (*specifications.Specification, error) {

	idx := slices.IndexFunc(dbm.SpecificationStructs, func(s specifications.Specification) bool { return s.Id == id })
	if idx == -1 {
		return nil, &errors.ElementNotFoundErr{Type: "specifications.Specification", Id: id}
	}
	specification := dbm.SpecificationStructs[idx]
	return &specification, nil
}

/**
GetPlanning function extracts a concrete Planning given its id. It takes as input
the identifier of the Planning. It returns the pointer of the Planning extracted
from the database and an error variable in charge of notifying any problem.
*/
func (dbm *DBMock) GetPlanning(id string) (*[][]string, error) {

	planning := dbm.PlanningOfSpecifications[id]
	if len(planning) == 0 {
		return nil, &errors.ElementNotFoundErr{Type: "Planning", Id: id}
	}
	return &planning, nil
}

/**
GetDefinition function extracts a concrete Definition given its id. It takes as input
the identifier of the Definition. It returns the pointer of the Definition extracted
from the database and an error variable in charge of notifying any problem.
*/
func (dbm *DBMock) GetDefinition(id string) (*definitions.Definition, error) {

	idx := slices.IndexFunc(dbm.DefinitionStructs, func(d definitions.Definition) bool { return d.Id == id })
	if idx == -1 {
		return nil, &errors.ElementNotFoundErr{Type: "definitions.Definition", Id: id}
	}
	definition := dbm.DefinitionStructs[idx]
	return &definition, nil
}

/**
GetResultDefinition function extracts a concrete ResultDefinition given its id. It takes as input
the identifier of the ResultDefinition. It returns the pointer of the ResultDefinition extracted
from the database and an error variable in charge of notifying any problem.
*/
func (dbm *DBMock) GetResultDefinition(id string) (*results.ResultDefinition, error) {

	idx := slices.IndexFunc(dbm.ResultDefinitionStructs, func(rd results.ResultDefinition) bool { return rd.Id == id })
	if idx == -1 {
		return nil, &errors.ElementNotFoundErr{Type: "results.ResultDefinition", Id: id}
	}
	resultDefinition := dbm.ResultDefinitionStructs[idx]
	return &resultDefinition, nil
}

/**
AddComponent function inserts a given Component into the database. It takes as input
the pointer of the Component to be registered. It provides as output an error variable
in charge of notifying any problem.
*/
func (dbm *DBMock) AddComponent(component *components.Component) error {

	idx := slices.IndexFunc(dbm.ComponentStructs, func(c components.Component) bool { return c.Id == component.Id })
	if idx != -1 {
		return &errors.DuplicateIDErr{Type: "components.Component", Id: component.Id}
	}
	dbm.ComponentStructs = append(dbm.ComponentStructs, *component)
	return nil
}

/**
AddSpecification function inserts a given Specification into the database. It takes as input
the pointer of the Specification to be registered. It provides as output an error variable
in charge of notifying any problem.
*/
func (dbm *DBMock) AddSpecification(specification *specifications.Specification) error {

	idx := slices.IndexFunc(dbm.SpecificationStructs, func(s specifications.Specification) bool { return s.Id == specification.Id })
	if idx != -1 {
		return &errors.DuplicateIDErr{Type: "specifications.Specification", Id: specification.Id}
	}
	dbm.SpecificationStructs = append(dbm.SpecificationStructs, *specification)
	return nil
}

/**
AddPlanning function inserts a given Planning into the database. It takes as input
the pointer of the Planning to be registered. It provides as output an error variable
in charge of notifying any problem.
*/
func (dbm *DBMock) AddPlanning(planning *[][]string, specificationId string) error {

	if _, exists := dbm.PlanningOfSpecifications[specificationId]; exists {
		return &errors.DuplicateIDErr{Type: "Planning", Id: specificationId}
	}
	dbm.PlanningOfSpecifications[specificationId] = *planning
	return nil
}

/**
AddDefinition function inserts a given Definition into the database. It takes as input
the pointer of the Definition to be registered. It provides as output an error variable
in charge of notifying any problem.
*/
func (dbm *DBMock) AddDefinition(definition *definitions.Definition) error {

	idx := slices.IndexFunc(dbm.DefinitionStructs, func(d definitions.Definition) bool { return d.Id == definition.Id })
	if idx != -1 {
		return &errors.DuplicateIDErr{Type: "definitions.Definition", Id: definition.Id}
	}
	dbm.DefinitionStructs = append(dbm.DefinitionStructs, *definition)
	return nil
}

/**
AddResultDefinition function inserts a given ResultDefinition into the database. It takes as input
the pointer of the ResultDefinition to be registered. It provides as output an error variable
in charge of notifying any problem.
*/
func (dbm *DBMock) AddResultDefinition(resultDefinition *results.ResultDefinition) error {

	idx := slices.IndexFunc(dbm.ResultDefinitionStructs, func(rd results.ResultDefinition) bool { return rd.Id == resultDefinition.Id })
	if idx != -1 {
		return &errors.DuplicateIDErr{Type: "results.ResultDefinition", Id: resultDefinition.Id}
	}
	dbm.ResultDefinitionStructs = append(dbm.ResultDefinitionStructs, *resultDefinition)
	return nil
}

/**
UpdateResultJob function updates a given ResultJob in the database by modifying its content in
the relevant ResultDefinition. It takes as input the pointer of the ResultJob and the identifier
of the ResultDefinition to which it belongs. It provides as output an error variable in charge
of notifying any problem.
*/
func (dbm *DBMock) UpdateResultJob(resultJob *results.ResultJob, resultDefinitionId string) error {

	idxResultDefinition := slices.IndexFunc(dbm.ResultDefinitionStructs, func(rd results.ResultDefinition) bool { return rd.Id == resultDefinitionId })
	if idxResultDefinition == -1 {
		return &errors.ElementNotFoundErr{Type: "results.ResultDefinition", Id: resultDefinitionId}
	}
	resultDefinition := dbm.ResultDefinitionStructs[idxResultDefinition]
	idxResultJob := slices.IndexFunc(resultDefinition.ResultJobs, func(jobRes results.ResultJob) bool { return jobRes.Id == resultJob.Id })
	if idxResultJob == -1 {
		resultDefinition.ResultJobs = append(resultDefinition.ResultJobs, *resultJob)
	} else {
		resultDefinition.ResultJobs[idxResultJob] = *resultJob
	}
	dbm.ResultDefinitionStructs[idxResultDefinition] = resultDefinition

	return nil
}

/**
GetDefinitionsWithWaitings returns those definitions where some of their tasks are pending
execution. Returns a pointer to the resulting list of definitions and an error variable in
charge of notifying any problem.
*/
func (dbm *DBMock) GetDefinitionsWithWaitings() (*[]definitions.Definition, error) {

	var res []definitions.Definition

	for _, resDef := range dbm.ResultDefinitionStructs {
		idxSomeWaiting := slices.IndexFunc(resDef.ResultJobs, func(jobRes results.ResultJob) bool { return jobRes.Status == results.Waiting })
		if idxSomeWaiting != -1 {
			def, err := dbm.GetDefinition(resDef.Id)
			if err != nil {
				return nil, fmt.Errorf("error during definition extraction %s", err.Error())
			}
			res = append(res, *def)
		}
	}

	return &res, nil
}
