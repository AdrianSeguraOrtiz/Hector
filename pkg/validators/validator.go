package validators

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/specifications"
	"errors"
	"reflect"

	"github.com/go-playground/validator/v10"
	"golang.org/x/exp/slices"
)

type Validator struct {
	Validator *validator.Validate
}

// Creamos un constructor específico para nuestro problema
func NewValidator() *Validator {
	val := Validator{}

	v := validator.New()
	v.RegisterValidation("representsType", components.RepresentsType)
	v.RegisterValidation("validDependencies", specifications.ValidDependencies)

	val.Validator = v

	return &val
}

func (val *Validator) ValidateComponentStruct(componentPointer *components.Component) error {
	v := val.Validator
	componentErr := v.Struct(*componentPointer)
	return componentErr
}

func (val *Validator) ValidateSpecificationStruct(specificationPointer *specifications.Specification) error {
	v := val.Validator
	specificationErr := v.Struct(*specificationPointer)
	return specificationErr
}

func (val *Validator) ValidateDefinitionStruct(definitionPointer *definitions.Definition) error {
	v := val.Validator
	definitionErr := v.Struct(*definitionPointer)
	return definitionErr
}

func (val *Validator) ValidateDefinitionTaskNames(definitionTaskArrayPointer *[]definitions.DefinitionTask, specificationTaskArrayPointer *[]specifications.SpecificationTask) error {
	// For each task defined in the specification, its specification in the definition file is checked.
	for _, specificationTask := range *specificationTaskArrayPointer {
		idxDefinitionTask := slices.IndexFunc(*definitionTaskArrayPointer, func(t definitions.DefinitionTask) bool { return t.Name == specificationTask.Name })
		if idxDefinitionTask == -1 {
			return errors.New("Task " + specificationTask.Name + " is required in the selected specification but is not present in the definition file.")
		}
	}
	return nil
}

func (val *Validator) ValidateDefinitionParameters(definitionParameterArrayPointer *[]definitions.Parameter, specificationPutArrayPointer *[]components.Put) error {
	// For each parameter defined in the specification, it is checked that the definition file includes it and associates a valid value for it.
	for _, componentPut := range *specificationPutArrayPointer {
		idxDefinitionParameter := slices.IndexFunc(*definitionParameterArrayPointer, func(p definitions.Parameter) bool { return p.Name == componentPut.Name })
		if idxDefinitionParameter == -1 {
			return errors.New("Parameter " + componentPut.Name + " is required but is not present in the definition file.")
		}
		definitionParameter := (*definitionParameterArrayPointer)[idxDefinitionParameter]
		if reflect.TypeOf(definitionParameter.Value).String() != componentPut.Type {
			return errors.New("Parameter " + componentPut.Name + " has an invalid value in the definition file.")
		}
	}
	return nil
}
