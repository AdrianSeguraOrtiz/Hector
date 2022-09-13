package validators

import (
	"errors"
	"testing"
	"encoding/json"
	"path/filepath"
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/workflows"
	"dag/hector/golang/module/pkg/executions"
	"dag/hector/golang/module/pkg/validators"
)

func TestValidateComponentStruct(t *testing.T) {
	var tests = []struct {
		componentFile string
		want string
	}{
		{"./../../data/hector/test_components/bad_component_1.json", "Key: 'Component.Id' Error:Field validation for 'Id' failed on the 'required' tag"},
		{"./../../data/hector/test_components/bad_component_2.json", "Key: 'Component.Inputs[1].Type' Error:Field validation for 'Type' failed on the 'required' tag"},
		{"./../../data/hector/test_components/bad_component_3.json", "Key: 'Component.Container.Image' Error:Field validation for 'Image' failed on the 'required' tag"}, 
		{"./../../data/hector/test_components/bad_component_4.json", "Key: 'Component.Outputs[0].Type' Error:Field validation for 'Type' failed on the 'representsType' tag"}, 
		{"./../../data/hector/test_components/good_component.json", ""}, 
	}

	for _, tt := range tests {

		testname := filepath.Base(tt.componentFile)
		t.Run(testname, func(t *testing.T) {
			componentByteValue, _ := pkg.ReadFile(tt.componentFile)
			var component components.Component
			json.Unmarshal(componentByteValue, &component)

			validator := validators.Validator{}
			componentErr := validator.ValidateComponentStruct(&component)

			if componentErr == nil {componentErr = errors.New("")}
			if componentErr.Error() != tt.want {
				t.Error("got ", componentErr, ", want ", tt.want)
			}
		})
	}
}


func TestValidateWorkflowStruct(t *testing.T) {
	var tests = []struct {
		workflowFile string
		want string
	}{
		{"./../../data/hector/test_workflows/bad_workflow_1.json", "Key: 'Workflow.Spec.Dag.Tasks[1].Component' Error:Field validation for 'Component' failed on the 'required' tag"},
		{"./../../data/hector/test_workflows/bad_workflow_2.json", "Key: 'Workflow.Id' Error:Field validation for 'Id' failed on the 'required' tag"},
		{"./../../data/hector/test_workflows/bad_workflow_3.json", "Key: 'Workflow.Spec.Dag.Tasks' Error:Field validation for 'Tasks' failed on the 'validDependencies' tag"},
		{"./../../data/hector/test_workflows/good_workflow.json", ""}, 
	}

	for _, tt := range tests {

		testname := filepath.Base(tt.workflowFile)
		t.Run(testname, func(t *testing.T) {
			workflowByteValue, _ := pkg.ReadFile(tt.workflowFile)
			var workflow workflows.Workflow
			json.Unmarshal(workflowByteValue, &workflow)

			validator := validators.Validator{}
			workflowErr := validator.ValidateWorkflowStruct(&workflow)

			if workflowErr == nil {workflowErr = errors.New("")}
			if workflowErr.Error() != tt.want {
				t.Error("got ", workflowErr, ", want ", tt.want)
			}
		})
	}
}

func TestValidateExecutionStruct(t *testing.T) {
	var tests = []struct {
		executionFile string
		want string
	}{
		{"./../../data/hector/test_executions/bad_execution_1.json", "Key: 'Execution.Data.Tasks[0].Inputs[1].Name' Error:Field validation for 'Name' failed on the 'required' tag"},
		{"./../../data/hector/test_executions/bad_execution_2.json", "Key: 'Execution.Data.Tasks[2].Inputs[0].Value' Error:Field validation for 'Value' failed on the 'required' tag"},
		{"./../../data/hector/test_executions/bad_execution_3.json", "Key: 'Execution.Data.Tasks[1].Name' Error:Field validation for 'Name' failed on the 'required' tag"},
		{"./../../data/hector/test_executions/good_execution.json", ""}, 
	}

	for _, tt := range tests {

		testname := filepath.Base(tt.executionFile)
		t.Run(testname, func(t *testing.T) {
			executionByteValue, _ := pkg.ReadFile(tt.executionFile)
			var execution executions.Execution
			json.Unmarshal(executionByteValue, &execution)

			validator := validators.Validator{}
			executionErr := validator.ValidateExecutionStruct(&execution)

			if executionErr == nil {executionErr = errors.New("")}
			if executionErr.Error() != tt.want {
				t.Error("got ", executionErr, ", want ", tt.want)
			}
		})
	}
}

func TestValidateExecutionTaskNames(t *testing.T) {
	var tests = []struct {
		executionFile string
		want string
	}{
		{"./../../data/hector/test_executions_task_names/bad_execution_task_names_1.json", "Task Concat Messages 1 is required in the selected workflow but is not present in the execution file."},
		{"./../../data/hector/test_executions_task_names/bad_execution_task_names_2.json", "Task Concat Messages 2 is required in the selected workflow but is not present in the execution file."},
		{"./../../data/hector/test_executions_task_names/bad_execution_task_names_3.json", "Task Count Letters is required in the selected workflow but is not present in the execution file."},
		{"./../../data/hector/test_executions_task_names/good_execution_task_names.json", ""}, 
	}

	workflowByteValue, _ := pkg.ReadFile("./../../data/hector/test_executions_task_names/reference_workflow.json")
	var workflow workflows.Workflow
	json.Unmarshal(workflowByteValue, &workflow)

	for _, tt := range tests {

		testname := filepath.Base(tt.executionFile)
		t.Run(testname, func(t *testing.T) {
			executionByteValue, _ := pkg.ReadFile(tt.executionFile)
			var execution executions.Execution
			json.Unmarshal(executionByteValue, &execution)

			validator := validators.Validator{}
			taskValidatorErr := validator.ValidateExecutionTaskNames(&execution.Data.Tasks, &workflow.Spec.Dag.Tasks)

			if taskValidatorErr == nil {taskValidatorErr = errors.New("")}
			if taskValidatorErr.Error() != tt.want {
				t.Error("got ", taskValidatorErr, ", want ", tt.want)
			}
		})
	}
}

func TestValidateExecutionParameters(t *testing.T) {
	var tests = []struct {
		executionTaskFile string
		want string
	}{
		{"./../../data/hector/test_executions_parameters/bad_task_execution_1.json", "Parameter input-file-1 is required but is not present in the execution file."},
		{"./../../data/hector/test_executions_parameters/bad_task_execution_2.json", "Parameter input-file-2 has an invalid value in the execution file."},
		{"./../../data/hector/test_executions_parameters/bad_task_execution_3.json", "Parameter output-file is required but is not present in the execution file."},
		{"./../../data/hector/test_executions_parameters/good_task_execution.json", ""}, 
	}

	componentByteValue, _ := pkg.ReadFile("./../../data/hector/test_executions_parameters/reference_component.json")
	var component components.Component
	json.Unmarshal(componentByteValue, &component)

	for _, tt := range tests {

		testname := filepath.Base(tt.executionTaskFile)
		t.Run(testname, func(t *testing.T) {
			executionTaskByteValue, _ := pkg.ReadFile(tt.executionTaskFile)
			var executionTask executions.ExecutionTask
			json.Unmarshal(executionTaskByteValue, &executionTask)

			validator := validators.Validator{}
			puts := append(executionTask.Inputs, executionTask.Outputs ...)
			parameters := append(component.Inputs, component.Outputs ...)
			putValidatorErr := validator.ValidateExecutionParameters(&puts, &parameters)

			if putValidatorErr == nil {putValidatorErr = errors.New("")}
			if putValidatorErr.Error() != tt.want {
				t.Error("got ", putValidatorErr, ", want ", tt.want)
			}
		})
	}
}