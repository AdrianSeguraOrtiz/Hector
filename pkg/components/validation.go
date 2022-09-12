package components

import (
	"github.com/go-playground/validator/v10"
	"dag/hector/golang/module/pkg"
)

func RepresentsType(fl validator.FieldLevel) bool {
	value := fl.Field().Interface().(string)
	types := []string{"string", "integer", "float", "bool"}
	return pkg.Contains(types, value)
}