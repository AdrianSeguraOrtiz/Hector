package workflows

import (
	"github.com/go-playground/validator/v10"
	"dag/hector/golang/module/pkg"
)

func ValidDependencies(fl validator.FieldLevel) bool {
	tasks := fl.Field().Interface().([]WorkflowTask)

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