package components

import (
	"dag/hector/golang/module/pkg"
	"encoding/json"

	"github.com/go-playground/validator/v10"
)

/**
RepresentsType function is responsible for validating that the type specified for the variable
is within the allowed ones. It takes as input a variable type validator.FieldLevel and returns
a boolean value.
*/
func RepresentsType(fl validator.FieldLevel) bool {
	value := fl.Field().Interface().(string)
	types := []string{"string", "int", "float", "bool"}
	return pkg.Contains(types, value)
}

type Put struct {
	Name string `json:"name" validate:"required"`
	Type string `json:"type" validate:"required,representsType"`
}

type Component struct {
	Id                  string   `json:"id" validate:"required"`
	Name                string   `json:"name" validate:"required"`
	ApiVersion          string   `json:"apiVersion" validate:"required"`
	Inputs              []Put    `json:"inputs" validate:"dive"`
	Outputs             []Put    `json:"outputs" validate:"dive"`
	ContainerDockerfile string   `json:"containerDockerfile" validate:"required"`
	ContainerImage      string   `json:"containerImage" validate:"required"`
	ContainerCommand    []string `json:"containerCommand"`
}

/**
String function is applied to Component variables and returns their content as a string.
*/
func (comp *Component) String() string {
	s, _ := json.MarshalIndent(comp, "", "  ")
	return string(s)
}

/**
FromFile function is applied on variables of type Component and it is in charge of dumping
the content of a file in this variable. It takes as input the path of the file and returns
an error type variable in charge of notifying any problem.
*/
func (comp *Component) FromFile(file string) error {
	content, err := pkg.ReadFile(file)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(content, comp); err != nil {
		return err
	}
	return nil
}
