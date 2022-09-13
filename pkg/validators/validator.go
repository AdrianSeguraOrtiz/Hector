package validators

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/workflows"
	"dag/hector/golang/module/pkg/executions"
	"github.com/go-playground/validator/v10"
	"golang.org/x/exp/slices"
	"errors"
	"reflect"
)

type Validator struct {}

func (val *Validator) ValidateComponentStruct(componentPointer *components.Component) error {
	// Initialize the validator
	v := validator.New()

	// Add custom validation function
	v.RegisterValidation("representsType", components.RepresentsType)

	// Validate
	componentErr := v.Struct(*componentPointer)
	return componentErr
}

func (val *Validator) ValidateWorkflowStruct(workflowPointer *workflows.Workflow) error {
	// Initialize the validator
	v := validator.New()

	// Add custom validation function
	v.RegisterValidation("validDependencies", workflows.ValidDependencies)

	// Validate
	workflowErr := v.Struct(*workflowPointer)
	return workflowErr
}

func (val *Validator) ValidateExecutionStruct(executionPointer *executions.Execution) error {
	// Initialize the validator
	v := validator.New()

	// Validate
	executionErr := v.Struct(*executionPointer)
	return executionErr
}

func (val *Validator) ValidateExecutionTaskNames(executionTaskArrayPointer *[]executions.ExecutionTask, workflowTaskArrayPointer *[]workflows.WorkflowTask) error {
	// For each task defined in the workflow, its specification in the execution file is checked.
	for _, workflowTask := range *workflowTaskArrayPointer {
		idxExecutionTask := slices.IndexFunc(*executionTaskArrayPointer, func(t executions.ExecutionTask) bool { return t.Name == workflowTask.Name })
		if idxExecutionTask == -1 {
			return errors.New("Task " + workflowTask.Name + " is required in the selected workflow but is not present in the execution file.")
		}
	}
	return nil
}

func (val *Validator) ValidateExecutionParameters(executionParameterArrayPointer *[]executions.Parameter, workflowPutArrayPointer *[]components.Put) error {
	// For each parameter defined in the workflow, it is checked that the execution file includes it and associates a valid value for it.
	for _, componentPut := range *workflowPutArrayPointer {
		idxExecutionParameter := slices.IndexFunc(*executionParameterArrayPointer, func(p executions.Parameter) bool { return p.Name == componentPut.Name })
		if idxExecutionParameter == -1 {
			return errors.New("Parameter " + componentPut.Name + " is required but is not present in the execution file.")
		}
		executionParameter := (*executionParameterArrayPointer)[idxExecutionParameter]
		if reflect.TypeOf(executionParameter.Value).String() != componentPut.Type {
			return errors.New("Parameter " + componentPut.Name + " has an invalid value in the execution file.")
		}
	}
	return nil
}