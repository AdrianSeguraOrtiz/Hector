package validators

import (
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/specifications"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
)

func TestValidateComponentStruct(t *testing.T) {
	var tests = []struct {
		componentFile string
		want          string
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
			var component components.Component
			component.FromFile(tt.componentFile)

			validator := NewValidator()
			componentErr := validator.ValidateComponentStruct(&component)

			if componentErr == nil {
				componentErr = fmt.Errorf("")
			}
			if componentErr.Error() != tt.want {
				t.Error("got ", componentErr, ", want ", tt.want)
			}
		})
	}
}

func TestValidateSpecificationStruct(t *testing.T) {
	var tests = []struct {
		specificationFile string
		want              string
	}{
		{"./../../data/hector/test_specifications/bad_specification_1.json", "Key: 'Specification.Spec.Dag.Tasks[1].Component' Error:Field validation for 'Component' failed on the 'required' tag"},
		{"./../../data/hector/test_specifications/bad_specification_2.json", "Key: 'Specification.Id' Error:Field validation for 'Id' failed on the 'required' tag"},
		{"./../../data/hector/test_specifications/bad_specification_3.json", "Key: 'Specification.Spec.Dag.Tasks' Error:Field validation for 'Tasks' failed on the 'validDependencies' tag"},
		{"./../../data/hector/test_specifications/good_specification.json", ""},
	}

	for _, tt := range tests {

		testname := filepath.Base(tt.specificationFile)
		t.Run(testname, func(t *testing.T) {
			var specification specifications.Specification
			specification.FromFile(tt.specificationFile)

			validator := NewValidator()
			specificationErr := validator.ValidateSpecificationStruct(&specification)

			if specificationErr == nil {
				specificationErr = fmt.Errorf("")
			}
			if specificationErr.Error() != tt.want {
				t.Error("got ", specificationErr, ", want ", tt.want)
			}
		})
	}
}

func TestValidateDefinitionStruct(t *testing.T) {
	var tests = []struct {
		definitionFile string
		want           string
	}{
		{"./../../data/hector/test_definitions/bad_definition_1.json", "Key: 'Definition.Data.Tasks[0].Inputs[1].Name' Error:Field validation for 'Name' failed on the 'required' tag"},
		{"./../../data/hector/test_definitions/bad_definition_2.json", "Key: 'Definition.Data.Tasks[2].Inputs[0].Value' Error:Field validation for 'Value' failed on the 'required' tag"},
		{"./../../data/hector/test_definitions/bad_definition_3.json", "Key: 'Definition.Data.Tasks[1].Name' Error:Field validation for 'Name' failed on the 'required' tag"},
		{"./../../data/hector/test_definitions/good_definition.json", ""},
	}

	for _, tt := range tests {

		testname := filepath.Base(tt.definitionFile)
		t.Run(testname, func(t *testing.T) {
			var definition definitions.Definition
			definition.FromFile(tt.definitionFile)

			validator := NewValidator()
			definitionErr := validator.ValidateDefinitionStruct(&definition)

			if definitionErr == nil {
				definitionErr = fmt.Errorf("")
			}
			if definitionErr.Error() != tt.want {
				t.Error("got ", definitionErr, ", want ", tt.want)
			}
		})
	}
}

func TestValidateDefinitionTaskNames(t *testing.T) {
	var tests = []struct {
		definitionFile string
		want           string
	}{
		{"./../../data/hector/test_definitions_task_names/bad_definition_task_names_1.json", "Task Concat Messages 1 is required in the selected specification but is not present in the definition file."},
		{"./../../data/hector/test_definitions_task_names/bad_definition_task_names_2.json", "Task Concat Messages 2 is required in the selected specification but is not present in the definition file."},
		{"./../../data/hector/test_definitions_task_names/bad_definition_task_names_3.json", "Task Count Letters is required in the selected specification but is not present in the definition file."},
		{"./../../data/hector/test_definitions_task_names/good_definition_task_names.json", ""},
	}

	var specification specifications.Specification
	specification.FromFile("./../../data/hector/test_definitions_task_names/reference_specification.json")

	for _, tt := range tests {

		testname := filepath.Base(tt.definitionFile)
		t.Run(testname, func(t *testing.T) {
			var definition definitions.Definition
			definition.FromFile(tt.definitionFile)

			validator := NewValidator()
			taskValidatorErr := validator.ValidateDefinitionTaskNames(&definition.Data.Tasks, &specification.Spec.Dag.Tasks)

			if taskValidatorErr == nil {
				taskValidatorErr = fmt.Errorf("")
			}
			if taskValidatorErr.Error() != tt.want {
				t.Error("got ", taskValidatorErr, ", want ", tt.want)
			}
		})
	}
}

func TestValidateDefinitionParameters(t *testing.T) {
	var tests = []struct {
		definitionTaskFile string
		want               string
	}{
		{"./../../data/hector/test_definitions_parameters/bad_task_definition_1.json", "Parameter input-file-1 is required but is not present in the definition file."},
		{"./../../data/hector/test_definitions_parameters/bad_task_definition_2.json", "Parameter input-file-2 has an invalid value in the definition file."},
		{"./../../data/hector/test_definitions_parameters/bad_task_definition_3.json", "Parameter output-file is required but is not present in the definition file."},
		{"./../../data/hector/test_definitions_parameters/good_task_definition.json", ""},
	}

	var component components.Component
	component.FromFile("./../../data/hector/test_definitions_parameters/reference_component.json")

	for _, tt := range tests {

		testname := filepath.Base(tt.definitionTaskFile)
		t.Run(testname, func(t *testing.T) {
			definitionTaskByteValue, _ := pkg.ReadFile(tt.definitionTaskFile)
			var definitionTask definitions.DefinitionTask
			json.Unmarshal(definitionTaskByteValue, &definitionTask)

			validator := NewValidator()
			puts := append(definitionTask.Inputs, definitionTask.Outputs...)
			parameters := append(component.Inputs, component.Outputs...)
			putValidatorErr := validator.ValidateDefinitionParameters(&puts, &parameters)

			if putValidatorErr == nil {
				putValidatorErr = fmt.Errorf("")
			}
			if putValidatorErr.Error() != tt.want {
				t.Error("got ", putValidatorErr, ", want ", tt.want)
			}
		})
	}
}
