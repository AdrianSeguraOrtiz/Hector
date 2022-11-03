package validators

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/specifications"
	"fmt"
	"reflect"

	"github.com/go-playground/validator/v10"
	"golang.org/x/exp/slices"
)

type Validator struct {
	Validator *validator.Validate
}

// We create a specific constructor for our problem
func NewValidator() *Validator {
	val := Validator{}

	v := validator.New()
	v.RegisterValidation("representsType", components.RepresentsType)
	v.RegisterValidation("validDependencies", specifications.ValidDependencies)

	val.Validator = v

	return &val
}

// Validate Component schema
func (val *Validator) ValidateComponentStruct(componentPointer *components.Component) error {
	v := val.Validator
	componentErr := v.Struct(*componentPointer)
	return componentErr
}

// Validate Specification schema
func (val *Validator) ValidateSpecificationStruct(specificationPointer *specifications.Specification) error {
	v := val.Validator
	specificationErr := v.Struct(*specificationPointer)
	return specificationErr
}

// Validate Definition schema
func (val *Validator) ValidateDefinitionStruct(definitionPointer *definitions.Definition) error {
	v := val.Validator
	definitionErr := v.Struct(*definitionPointer)
	return definitionErr
}

// For each task defined in the specification, its information in the definition file is checked.
func (val *Validator) ValidateDefinitionTaskNames(definitionTaskArrayPointer *[]definitions.DefinitionTask, specificationTaskArrayPointer *[]specifications.SpecificationTask) error {
	for _, specificationTask := range *specificationTaskArrayPointer {
		idxDefinitionTask := slices.IndexFunc(*definitionTaskArrayPointer, func(t definitions.DefinitionTask) bool { return t.Name == specificationTask.Name })
		if idxDefinitionTask == -1 {
			return fmt.Errorf("task %s is required in the selected specification but is not present in the definition file", specificationTask.Name)
		}
	}
	return nil
}

// For each parameter defined in the specification, it is checked that the definition file includes it and associates a valid value for it.
func (val *Validator) ValidateDefinitionParameters(definitionParameterArrayPointer *[]definitions.Parameter, specificationPutArrayPointer *[]components.Put) error {
	for _, componentPut := range *specificationPutArrayPointer {
		idxDefinitionParameter := slices.IndexFunc(*definitionParameterArrayPointer, func(p definitions.Parameter) bool { return p.Name == componentPut.Name })
		if idxDefinitionParameter == -1 {
			return fmt.Errorf("parameter %s is required but is not present in the definition file", componentPut.Name)
		}
		definitionParameter := (*definitionParameterArrayPointer)[idxDefinitionParameter]
		if reflect.TypeOf(definitionParameter.Value).String() != componentPut.Type {
			return fmt.Errorf("parameter %s has an invalid value in the definition file", componentPut.Name)
		}
	}
	return nil
}
