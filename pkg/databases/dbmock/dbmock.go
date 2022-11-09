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

// We create a specific constructor for our problem
func NewDBMock() *DBMock {
	db := DBMock{}
	db.PlanningOfSpecifications = make(map[string][][]string)
	return &db
}

func (dbm *DBMock) GetComponent(id string) (*components.Component, error) {
	/*
	   Performs a query to extract a component given its identifier
	*/

	idx := slices.IndexFunc(dbm.ComponentStructs, func(c components.Component) bool { return c.Id == id })
	if idx == -1 {
		return nil, &errors.ElementNotFoundErr{Type: "components.Component", Id: id}
	}
	component := dbm.ComponentStructs[idx]
	return &component, nil
}

func (dbm *DBMock) GetSpecification(id string) (*specifications.Specification, error) {
	/*
	   Performs a query to extract a specification given its identifier
	*/

	idx := slices.IndexFunc(dbm.SpecificationStructs, func(s specifications.Specification) bool { return s.Id == id })
	if idx == -1 {
		return nil, &errors.ElementNotFoundErr{Type: "specifications.Specification", Id: id}
	}
	specification := dbm.SpecificationStructs[idx]
	return &specification, nil
}

func (dbm *DBMock) GetPlanning(id string) (*[][]string, error) {
	/*
	   Performs a query to extract the planning of a specification given its identifier
	*/

	planning := dbm.PlanningOfSpecifications[id]
	if len(planning) == 0 {
		return nil, &errors.ElementNotFoundErr{Type: "Planning", Id: id}
	}
	return &planning, nil
}

func (dbm *DBMock) GetDefinition(id string) (*definitions.Definition, error) {
	/*
	   Performs a query to extract a definition given its identifier
	*/

	idx := slices.IndexFunc(dbm.DefinitionStructs, func(d definitions.Definition) bool { return d.Id == id })
	if idx == -1 {
		return nil, &errors.ElementNotFoundErr{Type: "definitions.Definition", Id: id}
	}
	definition := dbm.DefinitionStructs[idx]
	return &definition, nil
}

func (dbm *DBMock) GetResultDefinition(id string) (*results.ResultDefinition, error) {
	/*
	   Performs a query to extract a result definition given its identifier
	*/

	idx := slices.IndexFunc(dbm.ResultDefinitionStructs, func(rd results.ResultDefinition) bool { return rd.Id == id })
	if idx == -1 {
		return nil, &errors.ElementNotFoundErr{Type: "results.ResultDefinition", Id: id}
	}
	resultDefinition := dbm.ResultDefinitionStructs[idx]
	return &resultDefinition, nil
}

func (dbm *DBMock) AddComponent(component *components.Component) error {
	/*
	   Insert component in database
	*/

	idx := slices.IndexFunc(dbm.ComponentStructs, func(c components.Component) bool { return c.Id == component.Id })
	if idx != -1 {
		return &errors.DuplicateIDErr{Type: "components.Component", Id: component.Id}
	}
	dbm.ComponentStructs = append(dbm.ComponentStructs, *component)
	return nil
}

func (dbm *DBMock) AddSpecification(specification *specifications.Specification) error {
	/*
	   Insert specification in database
	*/

	idx := slices.IndexFunc(dbm.SpecificationStructs, func(s specifications.Specification) bool { return s.Id == specification.Id })
	if idx != -1 {
		return &errors.DuplicateIDErr{Type: "specifications.Specification", Id: specification.Id}
	}
	dbm.SpecificationStructs = append(dbm.SpecificationStructs, *specification)
	return nil
}

func (dbm *DBMock) AddPlanning(planning *[][]string, specificationId string) error {
	/*
	   Insert planning in database
	*/

	if _, exists := dbm.PlanningOfSpecifications[specificationId]; exists {
		return &errors.DuplicateIDErr{Type: "Planning", Id: specificationId}
	}
	dbm.PlanningOfSpecifications[specificationId] = *planning
	return nil
}

func (dbm *DBMock) AddDefinition(definition *definitions.Definition) error {
	/*
	   Insert definition in database
	*/

	idx := slices.IndexFunc(dbm.DefinitionStructs, func(d definitions.Definition) bool { return d.Id == definition.Id })
	if idx != -1 {
		return &errors.DuplicateIDErr{Type: "definitions.Definition", Id: definition.Id}
	}
	dbm.DefinitionStructs = append(dbm.DefinitionStructs, *definition)
	return nil
}

func (dbm *DBMock) AddResultDefinition(resultDefinition *results.ResultDefinition) error {
	/*
	   Insert result definition in database
	*/

	idx := slices.IndexFunc(dbm.ResultDefinitionStructs, func(rd results.ResultDefinition) bool { return rd.Id == resultDefinition.Id })
	if idx != -1 {
		return &errors.DuplicateIDErr{Type: "results.ResultDefinition", Id: resultDefinition.Id}
	}
	dbm.ResultDefinitionStructs = append(dbm.ResultDefinitionStructs, *resultDefinition)
	return nil
}

func (dbm *DBMock) UpdateResultJob(resultJob *results.ResultJob, resultDefinitionId string) error {
	/*
		Update Result Job into Result Definition in database
	*/

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

func (dbm *DBMock) GetDefinitionsWithWaitings() (*[]definitions.Definition, error) {
	/*
		Returns those definitions where some of their tasks are pending execution.
	*/

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