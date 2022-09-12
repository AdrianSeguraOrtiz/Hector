package main

import (
	"fmt"
    "encoding/json"
	"github.com/go-playground/validator/v10"

	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/workflows"
	"dag/hector/golang/module/pkg/executions"
)

func main() {

	// 1. Read json files and convert them to the corresponding struct

	// Component
    componentFile := "./data/hector/component_example.json"
	componentByteValue, err := pkg.ReadFile(componentFile)
	if err != nil {
		fmt.Println(err)
	}

	var component components.Component
	json.Unmarshal(componentByteValue, &component)

	fmt.Printf("Component: %+v\n\n", component)

	// Workflow
	workflowFile := "./data/hector/workflow_example.json"
	workflowByteValue, err := pkg.ReadFile(workflowFile)
	if err != nil {
		fmt.Println(err)
	}

	var workflow workflows.Workflow
	json.Unmarshal(workflowByteValue, &workflow)

	fmt.Printf("Workflow: %+v\n\n", workflow)

	// Execution
	executionFile := "./data/hector/execution_example.json"
	executionByteValue, err := pkg.ReadFile(executionFile)
	if err != nil {
		fmt.Println(err)
	}

	var execution executions.Execution
	json.Unmarshal(executionByteValue, &execution)

	fmt.Printf("Execution: %+v\n\n", execution)

	// We check that the type of the value is correctly inferred
	x := execution.Data.Tasks[0].Inputs[0].Value
	fmt.Println(x, ":", fmt.Sprintf("%T", x))

	y := execution.Data.Tasks[0].Inputs[1].Value
	fmt.Println(y, ":", fmt.Sprintf("%T", y))
	

	// 2. Validation

	// Initialize the validator
	v := validator.New()

	// Add custom validation functions
	v.RegisterValidation("representsType", components.RepresentsType)
	v.RegisterValidation("validDependencies", workflows.ValidDependencies)

	// Component
	componentErr := v.Struct(component)
	if componentErr != nil {
		fmt.Println(componentErr)
	}

	// Workflow
	workflowErr := v.Struct(workflow)
	if workflowErr != nil {
		fmt.Println(workflowErr)
	}

	// Execution
	executionErr := v.Struct(execution)
	if executionErr != nil {
		fmt.Println(executionErr)
	}


	// 3. Topological sorting
	orderedTasks := workflows.TopologicalGroupedSort(&workflow)
	fmt.Println()
	fmt.Println("Topological grouped order:", orderedTasks)
}