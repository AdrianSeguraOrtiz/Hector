package main

import (
    "fmt"
    "encoding/json"
    "dag/hector/golang/module/pkg"
    "dag/hector/golang/module/pkg/executions"
    "dag/hector/golang/module/pkg/validators"
    "dag/hector/golang/module/pkg/executors"
    "dag/hector/golang/module/pkg/executors/execgolang"
    "dag/hector/golang/module/pkg/databases"
    "dag/hector/golang/module/pkg/databases/dbmock"
    "flag"
)


func main() {
    // We obtain the path to the execution file provided as an input parameter
    var executionFile string
    flag.StringVar(&executionFile, "execution-file", "", "Execution json file path")
    flag.Parse()

    // We throw an error if not specified.
    if executionFile == "" {
        panic("Missing --execution-file flag")
    }

    // We read the execution json file
	executionByteValue, err := pkg.ReadFile(executionFile)
	pkg.Check(err)

    // Parse its content to the corresponding struct type
	var execution executions.Execution
	json.Unmarshal(executionByteValue, &execution)

    // We validate its structure
    validator := validators.Validator{}
    executionErr := validator.ValidateExecutionStruct(&execution)
    pkg.Check(executionErr)

    // We print its contents for visual verification
	fmt.Printf("Execution: %+v\n\n", execution)

    // Instantiate the database
    var database databases.Database
    database = &(dbmock.DBMock{})

    // Instantiate the executor
    var executor executors.Executor
    executor = &(execgolang.ExecGolang{})

    // Get jobs in topological order
    nestedJobs := databases.GetJobs(&execution, &database, &validator)

    // Execute jobs
    jobResults := executors.ExecuteJobs(&nestedJobs, &executor)

    // Create execution result
    resultExecution := executors.ResultExecution{
        Id: execution.Id,
        Name: execution.Name,
        Workflow: execution.Workflow,
        Jobs: jobResults,
    }

    // Add execution and executionResult to database
    database.AddExecution(&execution)
    database.AddResultExecution(&resultExecution)

}