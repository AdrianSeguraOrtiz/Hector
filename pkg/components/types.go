package components

import (
	"dag/hector/golang/module/pkg"
	"encoding/json"

	"github.com/go-playground/validator/v10"
)

func RepresentsType(fl validator.FieldLevel) bool {
	value := fl.Field().Interface().(string)
	types := []string{"string", "int", "float", "bool", "file"}
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

func (comp *Component) String() string {
	s, _ := json.MarshalIndent(comp, "", "  ")
	return string(s)
}

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
