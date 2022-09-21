package dbmock

import (
	"fmt"
    "encoding/json"
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/workflows"
    "dag/hector/golang/module/pkg/executions"
    "dag/hector/golang/module/pkg/executors"
    "dag/hector/golang/module/pkg/validators"
    "golang.org/x/exp/slices"
)

type DBMock struct {}

func (dbm *DBMock) GetWorkflow(id string) (workflows.Workflow, error) {
    /*
        Performs a query to extract a workflow given its identifier
    */

    idx := slices.IndexFunc(database.WorkflowStructs, func(w workflows.Workflow) bool { return w.Id == id })
    execWorkflow := database.WorkflowStructs[idx]
    return execWorkflow, nil
}

func (dbm *DBMock) GetTopologicalSort(id string) ([][]string, error) {
    /*
        Performs a query to extract the topological order of a workflow given its identifier
    */

    execTasksSorted := database.TopologicalSortOfWorkflows[id]
    return execTasksSorted, nil
}

func (dbm *DBMock) GetComponent(id string) (components.Component, error) {
    /*
        Performs a query to extract a component given its identifier
    */

    idx := slices.IndexFunc(database.ComponentStructs, func(c components.Component) bool { return c.Id == id })
    execComponent := database.ComponentStructs[idx]
    return execComponent, nil
}

func (dbm *DBMock) GetResultExecution(id string) (executors.ResultExecution, error) {
    /*
        Performs a query to extract a result execution given its identifier
    */

    idx := slices.IndexFunc(database.ResultExecutionStructs, func(re executors.ResultExecution) bool { return re.Id == id })
    resultExecution := database.ResultExecutionStructs[idx]
    return resultExecution, nil
}

func (dbm *DBMock) AddWorkflow(workflowPointer *workflows.Workflow) error {
    /*
        Insert workflow in database
    */

    database.WorkflowStructs = append(database.WorkflowStructs, *workflowPointer)
    return nil
}

func (dbm *DBMock) AddExecution(executionPointer *executions.Execution) error {
    /*
        Insert execution in database
    */

    database.ExecutionStructs = append(database.ExecutionStructs, *executionPointer)
    return nil
}

func (dbm *DBMock) AddResultExecution(resultExecutionPointer *executors.ResultExecution) error {
    /*
        Insert result execution in database
    */

    database.ResultExecutionStructs = append(database.ResultExecutionStructs, *resultExecutionPointer)
    return nil
}


// Mocking process

// We create a struct type to store the information that should be contained in the supposed database
type Database struct {
	ComponentStructs			[]components.Component
	WorkflowStructs				[]workflows.Workflow
    ExecutionStructs			[]executions.Execution
    ResultExecutionStructs		[]executors.ResultExecution
	TopologicalSortOfWorkflows	map[string][][]string
}

// We create a global variable of this type and we feed it by reading and validating local toy files
var database Database = mock()

// We create a function to encapsulate the whole process of reading and extraction of local data
func mock() (Database) {
    /*
        This function is responsible for extracting the list of components, workflows and topological sorts.
    */

	// Initialize the validator
	validator := validators.Validator{}

    // Components
    componentFiles := []string{"./data/hector/toy_components/concat_files/concat-files-component.json", "./data/hector/toy_components/concat_messages/concat-messages-component.json", "./data/hector/toy_components/count_letters/count-letters-component.json"}
    componentStructs := make([]components.Component, 0)

    for _, f := range componentFiles {
        componentByteValue, err := pkg.ReadFile(f)
        if err != nil {
            fmt.Println(err)
        }

        var component components.Component
        json.Unmarshal(componentByteValue, &component)

        componentErr := validator.ValidateComponentStruct(&component)
        if componentErr != nil {
            fmt.Println(componentErr)
        }

        componentStructs = append(componentStructs, component)
	}

    // Workflows
    workflowFiles := []string{"./data/hector/toy_workflows/toy_workflow_1.json"}
    workflowStructs := make([]workflows.Workflow, 0)

    for _, f := range workflowFiles {
        workflowByteValue, err := pkg.ReadFile(f)
        if err != nil {
            fmt.Println(err)
        }

        var workflow workflows.Workflow
        json.Unmarshal(workflowByteValue, &workflow)

        workflowErr := validator.ValidateWorkflowStruct(&workflow)
        if workflowErr != nil {
            fmt.Println(workflowErr)
        }

        workflowStructs = append(workflowStructs, workflow)
	}

    // Topological sorts
    topologicalSortOfWorkflows := make(map[string][][]string)
    for _, w := range workflowStructs {
        topologicalSortOfWorkflows[w.Id] = workflows.TopologicalGroupedSort(&w)
    }

    // Return database struct
	return Database {
		ComponentStructs: componentStructs, 
		WorkflowStructs: workflowStructs, 
		TopologicalSortOfWorkflows: topologicalSortOfWorkflows,
	}
}