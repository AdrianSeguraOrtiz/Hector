package definitions

import (
	"dag/hector/golang/module/pkg"
	"encoding/json"
)

type Parameter struct {
	Name  string      `json:"name" validate:"required"`
	Value interface{} `json:"value" validate:"required"`
}

type DefinitionTask struct {
	Name    string      `json:"name" validate:"required"`
	Inputs  []Parameter `json:"inputs" validate:"dive"`
	Outputs []Parameter `json:"outputs" validate:"dive"`
}

type Data struct {
	Tasks []DefinitionTask `json:"tasks" validate:"dive"`
}

type Definition struct {
	Id              string `json:"id" validate:"isdefault"`
	Name            string `json:"name" validate:"required"`
	SpecificationId string `json:"specificationId" validate:"required"`
	ApiVersion      string `json:"apiVersion" validate:"required"`
	Data            Data   `json:"data" validate:"dive"`
}

func (def *Definition) String() string {
	s, _ := json.MarshalIndent(def, "", "  ")
	return string(s)
}

func (def *Definition) FromFile(file string) error {
	content, err := pkg.ReadFile(file)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(content, def); err != nil {
		return err
	}
	return nil
}
