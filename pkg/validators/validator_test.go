package validators

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/specifications"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
)

func TestValidateComponentStruct(t *testing.T) {
	goodComponent := components.Component{
		Id:         "Component ID",
		Name:       "Component Name",
		ApiVersion: "hector/v1",
		Inputs: []components.Put{
			{
				Name: "input_1",
				Type: "string",
			},
			{
				Name: "input_2",
				Type: "int",
			},
		},
		Outputs: []components.Put{
			{
				Name: "output_1",
				Type: "string",
			},
		},
		ContainerDockerfile: "components/component_file.dockerfile",
		ContainerImage:      "image/name",
		ContainerCommand:    []string{"docker", "run", "..."},
	}
	strGoodComponent, _ := json.Marshal(goodComponent)

	badComponent1 := components.Component{}
	json.Unmarshal(strGoodComponent, &badComponent1)
	badComponent1.Inputs[1].Type = ""

	badComponent2 := components.Component{}
	json.Unmarshal(strGoodComponent, &badComponent2)
	badComponent2.ContainerImage = ""

	badComponent3 := components.Component{}
	json.Unmarshal(strGoodComponent, &badComponent3)
	badComponent3.Outputs[0].Type = "bad type"

	var tests = []struct {
		component *components.Component
		want      string
	}{
		{&badComponent1, "Key: 'Component.Inputs[1].Type' Error:Field validation for 'Type' failed on the 'required' tag"},
		{&badComponent2, "Key: 'Component.ContainerImage' Error:Field validation for 'ContainerImage' failed on the 'required' tag"},
		{&badComponent3, "Key: 'Component.Outputs[0].Type' Error:Field validation for 'Type' failed on the 'representsType' tag"},
		{&goodComponent, ""},
	}

	validator := NewValidator()

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			componentErr := validator.ValidateComponentStruct(tt.component)

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
	goodSpecification := specifications.Specification{
		Id:         "Specification ID",
		Name:       "Specification Name",
		ApiVersion: "hector/v1",
		Spec: specifications.Spec{
			Dag: specifications.Dag{
				Tasks: []specifications.SpecificationTask{
					{
						Name:      "A",
						Component: "Component 1 ID",
					},
					{
						Name:         "B",
						Dependencies: []string{"A"},
						Component:    "Component 2 ID",
					},
					{
						Name:         "C",
						Dependencies: []string{"A"},
						Component:    "Component 3 ID",
					},
					{
						Name:         "D",
						Dependencies: []string{"B", "C"},
						Component:    "Component 4 ID",
					},
				},
			},
		},
	}
	strGoodSpecification, _ := json.Marshal(goodSpecification)

	badSpecification1 := specifications.Specification{}
	json.Unmarshal(strGoodSpecification, &badSpecification1)
	badSpecification1.Spec.Dag.Tasks[1].Component = ""

	badSpecification2 := specifications.Specification{}
	json.Unmarshal(strGoodSpecification, &badSpecification2)
	badSpecification2.Id = ""

	badSpecification3 := specifications.Specification{}
	json.Unmarshal(strGoodSpecification, &badSpecification3)
	badSpecification3.Spec.Dag.Tasks[1].Dependencies = []string{"Unknown Task"}

	var tests = []struct {
		specification *specifications.Specification
		want          string
	}{
		{&badSpecification1, "Key: 'Specification.Spec.Dag.Tasks[1].Component' Error:Field validation for 'Component' failed on the 'required' tag"},
		{&badSpecification2, "Key: 'Specification.Id' Error:Field validation for 'Id' failed on the 'required' tag"},
		{&badSpecification3, "Key: 'Specification.Spec.Dag.Tasks' Error:Field validation for 'Tasks' failed on the 'validDependencies' tag"},
		{&goodSpecification, ""},
	}

	validator := NewValidator()

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			specificationErr := validator.ValidateSpecificationStruct(tt.specification)

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
	goodDefinition := definitions.Definition{
		Name:            "Definition Name",
		SpecificationId: "Specification ID",
		ApiVersion:      "hector/v1",
		Data: definitions.Data{
			Tasks: []definitions.DefinitionTask{
				{
					Name: "A",
					Inputs: []definitions.Parameter{
						{
							Name:  "input_1",
							Value: "A",
						},
						{
							Name:  "input_2",
							Value: 22,
						},
					},
					Outputs: []definitions.Parameter{
						{
							Name:  "output_1",
							Value: "path/to/output_file.csv",
						},
					},
				},
				{
					Name: "B",
					Inputs: []definitions.Parameter{
						{
							Name:  "input_1",
							Value: "B",
						},
					},
				},
				{
					Name: "C",
					Inputs: []definitions.Parameter{
						{
							Name:  "input_1",
							Value: "C",
						},
					},
				},
				{
					Name: "D",
					Inputs: []definitions.Parameter{
						{
							Name:  "input_1",
							Value: "D",
						},
					},
				},
			},
		},
	}
	strGoodDefinition, _ := json.Marshal(goodDefinition)

	badDefinition1 := definitions.Definition{}
	json.Unmarshal(strGoodDefinition, &badDefinition1)
	badDefinition1.Data.Tasks[0].Inputs[1].Name = ""

	badDefinition2 := definitions.Definition{}
	json.Unmarshal(strGoodDefinition, &badDefinition2)
	badDefinition2.Data.Tasks[2].Inputs[0].Value = nil

	badDefinition3 := definitions.Definition{}
	json.Unmarshal(strGoodDefinition, &badDefinition3)
	badDefinition3.Data.Tasks[1].Name = ""

	var tests = []struct {
		definition *definitions.Definition
		want       string
	}{
		{&badDefinition1, "Key: 'Definition.Data.Tasks[0].Inputs[1].Name' Error:Field validation for 'Name' failed on the 'required' tag"},
		{&badDefinition2, "Key: 'Definition.Data.Tasks[2].Inputs[0].Value' Error:Field validation for 'Value' failed on the 'required' tag"},
		{&badDefinition3, "Key: 'Definition.Data.Tasks[1].Name' Error:Field validation for 'Name' failed on the 'required' tag"},
		{&goodDefinition, ""},
	}

	validator := NewValidator()

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			definitionErr := validator.ValidateDefinitionStruct(tt.definition)

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
	referenceSpecification := specifications.Specification{
		Id:         "Specification ID",
		Name:       "Specification Name",
		ApiVersion: "hector/v1",
		Spec: specifications.Spec{
			Dag: specifications.Dag{
				Tasks: []specifications.SpecificationTask{
					{
						Name: "A",
					},
					{
						Name: "B",
					},
				},
			},
		},
	}

	goodDefinitionTaskNames := definitions.Definition{
		Name:            "Definition Name",
		SpecificationId: "Specification ID",
		ApiVersion:      "hector/v1",
		Data: definitions.Data{
			Tasks: []definitions.DefinitionTask{
				{
					Name: "A",
				},
				{
					Name: "B",
				},
			},
		},
	}

	strGoodDefinitionTaskNames, _ := json.Marshal(goodDefinitionTaskNames)

	badDefinitionTaskNames1 := definitions.Definition{}
	json.Unmarshal(strGoodDefinitionTaskNames, &badDefinitionTaskNames1)
	badDefinitionTaskNames1.Data.Tasks[0] = definitions.DefinitionTask{}

	badDefinitionTaskNames2 := definitions.Definition{}
	json.Unmarshal(strGoodDefinitionTaskNames, &badDefinitionTaskNames2)
	badDefinitionTaskNames2.Data.Tasks[1].Name = "Bad Task Name"

	var tests = []struct {
		definition *definitions.Definition
		want       string
	}{
		{&badDefinitionTaskNames1, "task A is required in the selected specification but is not present in the definition file"},
		{&badDefinitionTaskNames2, "task B is required in the selected specification but is not present in the definition file"},
		{&goodDefinitionTaskNames, ""},
	}

	validator := NewValidator()

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			taskValidatorErr := validator.ValidateDefinitionTaskNames(&tt.definition.Data.Tasks, &referenceSpecification.Spec.Dag.Tasks)

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
	referenceComponent := components.Component{
		Id:         "Component ID",
		Name:       "Component Name",
		ApiVersion: "hector/v1",
		Inputs: []components.Put{
			{
				Name: "input_1",
				Type: "string",
			},
			{
				Name: "input_2",
				Type: "int",
			},
		},
		Outputs: []components.Put{
			{
				Name: "output_1",
				Type: "string",
			},
		},
		ContainerDockerfile: "components/component_file.dockerfile",
		ContainerImage:      "image/name",
		ContainerCommand:    []string{"docker", "run", "..."},
	}

	goodTaskDefinition := definitions.DefinitionTask{
		Name: "Task Name",
		Inputs: []definitions.Parameter{
			{
				Name:  "input_1",
				Value: "test string",
			},
			{
				Name:  "input_2",
				Value: 22,
			},
		},
		Outputs: []definitions.Parameter{
			{
				Name:  "output_1",
				Value: "output_file.txt",
			},
		},
	}

	/**
	After marshalling, the int type is converted to byte and then the unmarshall converts it to float.
	This is why after each unmarshall it will be necessary to recover the original type of the int.
	*/
	strGoodTaskDefinition, _ := json.Marshal(goodTaskDefinition)

	badTaskDefinition1 := definitions.DefinitionTask{}
	json.Unmarshal(strGoodTaskDefinition, &badTaskDefinition1)
	badTaskDefinition1.Inputs[1].Value = int(badTaskDefinition1.Inputs[1].Value.(float64))
	badTaskDefinition1.Inputs[0].Name = "Bad Name"

	badTaskDefinition2 := definitions.DefinitionTask{}
	json.Unmarshal(strGoodTaskDefinition, &badTaskDefinition2)
	badTaskDefinition2.Inputs[1].Value = "test"

	badTaskDefinition3 := definitions.DefinitionTask{}
	json.Unmarshal(strGoodTaskDefinition, &badTaskDefinition3)
	badTaskDefinition3.Inputs[1].Value = int(badTaskDefinition3.Inputs[1].Value.(float64))
	badTaskDefinition3.Outputs[0] = definitions.Parameter{}

	var tests = []struct {
		definitionTask *definitions.DefinitionTask
		want           string
	}{
		{&badTaskDefinition1, "parameter input_1 is required but is not present in the definition file"},
		{&badTaskDefinition2, "parameter input_2 has an invalid value in the definition file"},
		{&badTaskDefinition3, "parameter output_1 is required but is not present in the definition file"},
		{&goodTaskDefinition, ""},
	}

	validator := NewValidator()
	parameters := append(referenceComponent.Inputs, referenceComponent.Outputs...)

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			puts := append(tt.definitionTask.Inputs, tt.definitionTask.Outputs...)
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
