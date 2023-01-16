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

/**
NewValidator function creates a new instance of the Validator type. It returns a pointer
to the constructed variable.
*/
func NewValidator() *Validator {
	val := Validator{}

	v := validator.New()
	v.RegisterValidation("representsType", components.RepresentsType)
	v.RegisterValidation("validDependencies", specifications.ValidDependencies)

	val.Validator = v

	return &val
}

/**
ValidateComponentStruct function is responsible for validating the content of a Component. It takes
as input the pointer to the Component and returns an error variable in charge of notifying any problem.
*/
func (val *Validator) ValidateComponentStruct(component *components.Component) error {
	v := val.Validator
	componentErr := v.Struct(*component)
	return componentErr
}

/**
ValidateSpecificationStruct function is responsible for validating the content of a Specification. It takes
as input the pointer to the Specification and returns an error variable in charge of notifying any problem.
*/
func (val *Validator) ValidateSpecificationStruct(specification *specifications.Specification) error {
	v := val.Validator
	specificationErr := v.Struct(*specification)
	return specificationErr
}

/**
ValidateDefinitionStruct function is responsible for validating the content of a Definition. It takes
as input the pointer to the Definition and returns an error variable in charge of notifying any problem.
*/
func (val *Validator) ValidateDefinitionStruct(definition *definitions.Definition) error {
	v := val.Validator
	definitionErr := v.Struct(*definition)
	return definitionErr
}

/**
ValidateDefinitionTaskNames function ensures the concordance between the name of the tasks provided
in the Definition and those stored in the corresponding Specification. It takes as input a pointer
to the array of tasks from the definition and a pointer to the array of tasks from the specification.
It returns an error variable in charge of notifying any problem.
*/
func (val *Validator) ValidateDefinitionTaskNames(definitionTaskArray *[]definitions.DefinitionTask, specificationTaskArray *[]specifications.SpecificationTask) error {
	for _, specificationTask := range *specificationTaskArray {
		idxDefinitionTask := slices.IndexFunc(*definitionTaskArray, func(t definitions.DefinitionTask) bool { return t.Name == specificationTask.Name })
		if idxDefinitionTask == -1 {
			return fmt.Errorf("task %s is required in the selected specification but is not present in the definition file", specificationTask.Name)
		}
	}
	return nil
}

/**
ValidateDefinitionParameters function checks the agreement between the parameters set in the definition
with those stored in the corresponding specification. It ensures the proper presence of names and that
the value entered in the definition is of the appropriate type. It takes as input a pointer to the array
of parameters from the definition and a pointer to the array of parameters from the specification. It
returns an error variable in charge of notifying any problem.
*/
func (val *Validator) ValidateDefinitionParameters(definitionParameterArray *[]definitions.Parameter, specificationPutArray *[]components.Put) error {
	for _, componentPut := range *specificationPutArray {
		idxDefinitionParameter := slices.IndexFunc(*definitionParameterArray, func(p definitions.Parameter) bool { return p.Name == componentPut.Name })
		if idxDefinitionParameter == -1 {
			return fmt.Errorf("parameter %s is required but is not present in the definition file", componentPut.Name)
		}
		definitionParameter := (*definitionParameterArray)[idxDefinitionParameter]
		if reflect.TypeOf(definitionParameter.Value).String() != componentPut.Type {
			return fmt.Errorf("parameter %s has an invalid value in the definition file", componentPut.Name)
		}
	}
	return nil
}
