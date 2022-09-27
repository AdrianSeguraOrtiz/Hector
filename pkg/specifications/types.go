package specifications

import (
	"dag/hector/golang/module/pkg"
	"io/ioutil"
	"net/http"

	"encoding/json"

	"github.com/go-playground/validator/v10"
)

func ValidDependencies(fl validator.FieldLevel) bool {
	tasks := fl.Field().Interface().([]SpecificationTask)

	var names []string
	for _, task := range tasks {
		names = append(names, task.Name)
	}

	for _, task := range tasks {
		for _, dependencie := range task.Dependencies {
			if !pkg.Contains(names, dependencie) {
				return false
			}
		}
	}

	return true
}

type SpecificationTask struct {
	Name         string   `json:"name" validate:"required"`
	Dependencies []string `json:"dependencies"`
	Component    string   `json:"component" validate:"required"`
}

type Dag struct {
	Tasks []SpecificationTask `json:"tasks" validate:"required,min=1,validDependencies,dive"`
}

type Spec struct {
	Dag Dag `json:"dag" validate:"required"`
}

type Specification struct {
	Id         string `json:"id" validate:"required"`
	Name       string `json:"name" validate:"required"`
	ApiVersion string `json:"apiVersion" validate:"required"`
	Spec       Spec   `json:"spec" validate:"required"`
}

func (spec *Specification) String() string {
	s, _ := json.MarshalIndent(spec, "", "  ")
	return string(s)
}

func (spec *Specification) FromFile(file string) error {
	content, err := pkg.ReadFile(file)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(content, spec); err != nil {
		return err
	}
	return nil
}

func (spec *Specification) FromRequest(req *http.Request) error {
	content, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(content, spec); err != nil {
		return err
	}
	return nil
}
