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

// String function is applied to Definition variables and returns their content as a string.
func (def *Definition) String() string {
	s, _ := json.MarshalIndent(def, "", "  ")
	return string(s)
}

// FromFile function is applied on variables of type Definition and it is in charge of dumping
// the content of a file in this variable. It takes as input the path of the file and returns
// an error type variable in charge of notifying any problem.
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
