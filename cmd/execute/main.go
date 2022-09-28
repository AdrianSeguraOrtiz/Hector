package main

import (
	"dag/hector/golang/module/pkg/databases"
	"dag/hector/golang/module/pkg/databases/dbmock"
	"dag/hector/golang/module/pkg/databases/sqlite3"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/executors"
	"dag/hector/golang/module/pkg/executors/execgolang"
	"dag/hector/golang/module/pkg/results"
	"dag/hector/golang/module/pkg/validators"
	"flag"
	"fmt"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	// We obtain the path to the definition file provided as an input parameter
	var definitionFile string
	flag.StringVar(&definitionFile, "definition-file", "", "Definition json file path")
	flag.Parse()

	// We throw an error if not specified.
	if definitionFile == "" {
		panic("Missing --definition-file flag")
	}

	// Parse its content to the corresponding struct type
	var definition definitions.Definition
	fileErr := definition.FromFile(definitionFile)
	check(fileErr)

	// We validate its structure
	validatorPointer := validators.NewValidator()
	definitionErr := (*validatorPointer).ValidateDefinitionStruct(&definition)
	check(definitionErr)

	// We print its contents for visual verification
	fmt.Println("Definition: ", definition.String())

	// Instantiate the database
	var database databases.Database
	database = dbmock.NewDBMock()

	// Add definition to database
	addDefErr := database.AddDefinition(&definition)
	check(addDefErr)

	// Instantiate the executor
	var executor executors.Executor
	executor = &(execgolang.ExecGolang{})

	// Get jobs in topological order
	nestedJobs, err := databases.GetJobs(&definition, &database, validatorPointer)
	check(err)

	// Get/Create definition result
	resultDefinition, err := database.GetResultDefinition(definition.Id)
	switch err.(type) {
	case *databases.ElementNotFoundErr:
		{
			fmt.Println(err.Error(), "A new document is created.")
			resultDefinition = results.ResultDefinition{
				Id:              definition.Id,
				Name:            definition.Name,
				SpecificationId: definition.SpecificationId,
				ResultJobs:      []results.ResultJob{},
			}
			err := database.AddResultDefinition(&resultDefinition)
			check(err)
		}
	default:
		check(err)
	}

	// Execute jobs
	fmt.Println("\nExecution:")
	resultDefinition.ResultJobs, err = executors.ExecuteJobs(&nestedJobs, &executor, &resultDefinition, &database)
	check(err)

	// Check sqlite3
	sqldb, err := sqlite3.NewSQLite3()
	check(err)

	AddErr := sqldb.AddResultDefinition(&resultDefinition)
	check(AddErr)
	rddb, err := sqldb.GetResultDefinition(resultDefinition.Id)
	check(err)

	// Print result definition
	fmt.Println(rddb.String())

}
