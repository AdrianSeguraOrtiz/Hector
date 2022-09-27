package dbmock

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/databases"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/results"
	"dag/hector/golang/module/pkg/specifications"
	"dag/hector/golang/module/pkg/validators"
	"errors"
	"fmt"

	"golang.org/x/exp/slices"
)

type DBMock struct{}

func (dbm *DBMock) GetComponent(id string) (components.Component, error) {
	/*
	   Performs a query to extract a component given its identifier
	*/

	idx := slices.IndexFunc(database.ComponentStructs, func(c components.Component) bool { return c.Id == id })
	if idx == -1 {
		return components.Component{}, &databases.ElementNotFoundErr{Type: "Component", Id: id}
	}
	component := database.ComponentStructs[idx]
	return component, nil
}

func (dbm *DBMock) GetSpecification(id string) (specifications.Specification, error) {
	/*
	   Performs a query to extract a specification given its identifier
	*/

	idx := slices.IndexFunc(database.SpecificationStructs, func(s specifications.Specification) bool { return s.Id == id })
	if idx == -1 {
		return specifications.Specification{}, &databases.ElementNotFoundErr{Type: "Specification", Id: id}
	}
	specification := database.SpecificationStructs[idx]
	return specification, nil
}

func (dbm *DBMock) GetTopologicalSort(id string) ([][]string, error) {
	/*
	   Performs a query to extract the topological order of a specification given its identifier
	*/

	planning := database.TopologicalSortOfSpecifications[id]
	if len(planning) == 0 {
		return nil, &databases.ElementNotFoundErr{Type: "Planning", Id: id}
	}
	return planning, nil
}

func (dbm *DBMock) GetDefinition(id string) (definitions.Definition, error) {
	/*
	   Performs a query to extract a definition given its identifier
	*/

	idx := slices.IndexFunc(database.SpecificationStructs, func(s specifications.Specification) bool { return s.Id == id })
	if idx == -1 {
		return definitions.Definition{}, &databases.ElementNotFoundErr{Type: "Definition", Id: id}
	}
	definition := database.DefinitionStructs[idx]
	return definition, nil
}

func (dbm *DBMock) GetResultDefinition(id string) (results.ResultDefinition, error) {
	/*
	   Performs a query to extract a result definition given its identifier
	*/

	idx := slices.IndexFunc(database.ResultDefinitionStructs, func(rd results.ResultDefinition) bool { return rd.Id == id })
	if idx == -1 {
		return results.ResultDefinition{}, &databases.ElementNotFoundErr{Type: "Result Definition", Id: id}
	}
	resultDefinition := database.ResultDefinitionStructs[idx]
	return resultDefinition, nil
}

func (dbm *DBMock) AddComponent(componentPointer *components.Component) error {
	/*
	   Insert component in database
	*/

	database.ComponentStructs = append(database.ComponentStructs, *componentPointer)
	return nil
}

func (dbm *DBMock) AddSpecification(specificationPointer *specifications.Specification) error {
	/*
	   Insert specification in database
	*/

	database.SpecificationStructs = append(database.SpecificationStructs, *specificationPointer)
	return nil
}

func (dbm *DBMock) AddTopologicalSort(planning [][]string, specificationId string) error {
	/*
	   Insert topological sort in database
	*/

	database.TopologicalSortOfSpecifications[specificationId] = planning
	return nil
}

func (dbm *DBMock) AddDefinition(definitionPointer *definitions.Definition) error {
	/*
	   Insert definition in database
	*/

	database.DefinitionStructs = append(database.DefinitionStructs, *definitionPointer)
	return nil
}

func (dbm *DBMock) AddResultDefinition(resultDefinitionPointer *results.ResultDefinition) error {
	/*
	   Insert result definition in database
	*/

	database.ResultDefinitionStructs = append(database.ResultDefinitionStructs, *resultDefinitionPointer)
	return nil
}

func (dbm *DBMock) UpdateResultJob(resultJobPointer *results.ResultJob, resultDefinitionId string) error {
	/*
		Update Result Job into Result Definition in database
	*/
	idxResultDefinition := slices.IndexFunc(database.ResultDefinitionStructs, func(rd results.ResultDefinition) bool { return rd.Id == resultDefinitionId })
	if idxResultDefinition == -1 {
		return errors.New("Could not find Result Definition in database")
	}
	resultDefinition := database.ResultDefinitionStructs[idxResultDefinition]
	idxResultJob := slices.IndexFunc(resultDefinition.Jobs, func(jobRes results.ResultJob) bool { return jobRes.Id == (*resultJobPointer).Id })
	if idxResultJob == -1 {
		resultDefinition.Jobs = append(resultDefinition.Jobs, *resultJobPointer)
	} else {
		resultDefinition.Jobs[idxResultJob] = *resultJobPointer
	}
	database.ResultDefinitionStructs[idxResultDefinition] = resultDefinition

	return nil
}

// Mocking process

// We create a struct type to store the information that should be contained in the supposed database
type Database struct {
	ComponentStructs                []components.Component
	SpecificationStructs            []specifications.Specification
	TopologicalSortOfSpecifications map[string][][]string
	DefinitionStructs               []definitions.Definition
	ResultDefinitionStructs         []results.ResultDefinition
}

// We create a global variable of this type and we feed it by reading and validating local toy files
var database Database = mock()

// We create a function to encapsulate the whole process of reading and extraction of local data
func mock() Database {
	/*
	   This function is responsible for extracting the list of components, specifications and topological sorts.
	*/

	// Initialize the validator
	validatorPointer := validators.NewValidator()

	// Components
	componentFiles := []string{"./data/hector/toy_components/concat_files/concat-files-component.json", "./data/hector/toy_components/concat_messages/concat-messages-component.json", "./data/hector/toy_components/count_letters/count-letters-component.json"}
	componentStructs := make([]components.Component, 0)

	for _, f := range componentFiles {
		var component components.Component
		component.FromFile(f)

		componentErr := validatorPointer.ValidateComponentStruct(&component)
		if componentErr != nil {
			fmt.Println(componentErr)
		}

		componentStructs = append(componentStructs, component)
	}

	// Specifications
	specificationFiles := []string{"./data/hector/toy_specifications/toy_specification_1.json"}
	specificationStructs := make([]specifications.Specification, 0)

	for _, f := range specificationFiles {
		var specification specifications.Specification
		specification.FromFile(f)

		specificationErr := validatorPointer.ValidateSpecificationStruct(&specification)
		if specificationErr != nil {
			fmt.Println(specificationErr)
		}

		specificationStructs = append(specificationStructs, specification)
	}

	// Topological sorts
	topologicalSortOfSpecifications := make(map[string][][]string)
	for _, w := range specificationStructs {
		topologicalSortOfSpecifications[w.Id] = specifications.TopologicalGroupedSort(&w)
	}

	// Return database struct
	return Database{
		ComponentStructs:                componentStructs,
		SpecificationStructs:            specificationStructs,
		TopologicalSortOfSpecifications: topologicalSortOfSpecifications,
	}
}
