package databases

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"dag/hector/golang/module/pkg/specifications"
	"dag/hector/golang/module/pkg/validators"

	"github.com/rs/xid"
	"golang.org/x/exp/slices"
)

type ElementNotFoundErr struct {
	Type string
	Id   string
}

func (e *ElementNotFoundErr) Error() string {
	return e.Type + " with id " + e.Id + " not found in database."
}

type DuplicateIDErr struct {
	Type string
	Id   string
}

func (e *DuplicateIDErr) Error() string {
	return "A " + e.Type + " with id " + e.Id + " is already stored in the database."
}

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
}

func GetJobs(definitionPointer *definitions.Definition, databasePointer *Database, validatorPointer *validators.Validator) ([][]jobs.Job, error) {
	// We extract the associated specification and its topological order
	specification, err := (*databasePointer).GetSpecification((*definitionPointer).SpecificationId)
	if err != nil {
		return nil, err
	}
	planning, err := (*databasePointer).GetTopologicalSort((*definitionPointer).SpecificationId)
	if err != nil {
		return nil, err
	}

	// We validate that the tasks required in the specification are specified in the definition file
	taskValidatorErr := (*validatorPointer).ValidateDefinitionTaskNames(&(*definitionPointer).Data.Tasks, &specification.Spec.Dag.Tasks)
	if taskValidatorErr != nil {
		return nil, taskValidatorErr
	}

	// We build a two-dimensional vector to store the topologically ordered tasks with the necessary content for their definition.
	var nestedJobs [][]jobs.Job

	// For each group of tasks ...
	for _, taskGroup := range planning {

		// One-dimensional vector for storing group tasks
		var jobsGroup []jobs.Job

		// For each task within the group ...
		for _, taskName := range taskGroup {

			// A. We extract the task information from the definition file
			idxDefinitionTask := slices.IndexFunc((*definitionPointer).Data.Tasks, func(t definitions.DefinitionTask) bool { return t.Name == taskName })
			definitionTask := (*definitionPointer).Data.Tasks[idxDefinitionTask]

			// B. We extract the task information from the specification struct (mainly to know the identifier of its component)
			idxSpecificationTask := slices.IndexFunc(specification.Spec.Dag.Tasks, func(t specifications.SpecificationTask) bool { return t.Name == taskName })
			specificationTask := specification.Spec.Dag.Tasks[idxSpecificationTask]
			componentId := specificationTask.Component

			// C. We extract the information about the task component
			execComponent, err := (*databasePointer).GetComponent(componentId)
			if err != nil {
				return nil, err
			}

			// D. We check that the parameters entered (inputs/outputs) in the definition file are correct
			inputValidatorErr := (*validatorPointer).ValidateDefinitionParameters(&definitionTask.Inputs, &execComponent.Inputs)
			if inputValidatorErr != nil {
				return nil, inputValidatorErr
			}
			outputValidatorErr := (*validatorPointer).ValidateDefinitionParameters(&definitionTask.Outputs, &execComponent.Outputs)
			if outputValidatorErr != nil {
				return nil, outputValidatorErr
			}

			// E. We create the definition task (job)
			job := jobs.Job{
				Id:           xid.New().String(),
				Name:         taskName,
				Image:        execComponent.Container.Image,
				Arguments:    append(definitionTask.Inputs, definitionTask.Outputs...),
				Dependencies: specificationTask.Dependencies,
			}

			// F. We add it to the group's task list
			jobsGroup = append(jobsGroup, job)
		}
		// We add the group's tasks to the two-dimensional list
		nestedJobs = append(nestedJobs, jobsGroup)
	}

	return nestedJobs, nil
}
