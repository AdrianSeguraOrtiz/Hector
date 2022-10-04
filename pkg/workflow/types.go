package workflow

import (
	"encoding/json"
	"io/ioutil"

	"github.com/go-playground/validator/v10"
)

type Task struct {
	Name      string   `json:"name" validate:"required"`
	Depends   []string `json:"dependencies"`
	Component string   `json:"component" validate:"required"`
}

type Dag struct {
	Tasks []Task `json:"tasks" validate:"required,min=1,validDependencies,dive"`
}

type Spec struct {
	Dag Dag `json:"dag" validate:"required"`
}

type WorkflowSpec struct {
	Id         string `json:"id" validate:"required"`
	Name       string `json:"name" validate:"required"`
	ApiVersion string `json:"apiVersion" validate:"required"`
	Spec       Spec   `json:"spec" validate:"required"`
}

type WorkflowDef struct {
}

func (wf *WorkflowSpec) String() string {
	s, _ := json.MarshalIndent(wf, "", "  ")
	return string(s)
}

func (wf *WorkflowSpec) FromFile(file string) error {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(content, wf); err != nil {
		return err
	}

	validate := validator.New()
	if err := validate.Struct(wf); err.(validator.ValidationErrors) != nil {
		return err
	}
	return nil
}
