package specifications

import (
	"dag/hector/golang/module/pkg"

	"encoding/json"

	"github.com/go-playground/validator/v10"
)

// ValidDependencies function is responsible for validating the dependencies registered in the
// specification. Specifically, the function checks that there is no dependency that has not been
// defined as a task in the specification itself. It takes as input a variable of type
// validator.FieldLevel and returns a boolean variable.
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

// String function is applied to Specification variables and returns their content as a string.
func (spec *Specification) String() string {
	s, _ := json.MarshalIndent(spec, "", "  ")
	return string(s)
}

// FromFile function is applied on variables of type Specification and it is in charge of dumping
// the content of a file in this variable. It takes as input the path of the file and returns
// an error type variable in charge of notifying any problem.
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
