package sqlite3

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/errors"
	"dag/hector/golang/module/pkg/results"
	"dag/hector/golang/module/pkg/specifications"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/exp/slices"
)

// We create a structured type with a pointer to the database
type SQLite3 struct {
	Database *sql.DB
}

// Element is an interface that encompasses all the types collected in the database.
type Element interface {
	components.Component | specifications.Specification | [][]string | definitions.Definition | results.ResultDefinition
}

// We declare all the prefixes of our table
type Prefix string

const (
	ComponentPrefix     Prefix = "comp-"
	SpecificationPrefix Prefix = "spec-"
	PlanningPrefix      Prefix = "plan-"
	DefinitionPrefix    Prefix = "def-"
	ResultDefPrefix     Prefix = "resdef-"
)

// We create a specific constructor for our problem
func NewSQLite3() (*SQLite3, error) {
	db := SQLite3{}

	sql, err := sql.Open("sqlite3", "hector.sqlite")
	if err != nil {
		return nil, err
	}

	strCreate := `
        CREATE TABLE IF NOT EXISTS hector(id TEXT PRIMARY KEY, content TEXT);
    `
	_, err = sql.Exec(strCreate)
	if err != nil {
		return nil, err
	}

	db.Database = sql

	return &db, nil
}

func genericGetFunction[V Element](dbsql *SQLite3, id string) (*V, error) {
	/*
	   Generic function for data extraction
	*/

	// Define the query
	strSelect := `SELECT content FROM hector WHERE id=?`

	// We prepare the request corresponding to the query
	statement, err := dbsql.Database.Prepare(strSelect)
	if err != nil {
		return nil, err
	}

	// We make sure to close the resource before the end of the function.
	defer statement.Close()

	// We create an empty string to later store the resulting data
	var content string

	// We create an empty struct to later unmarshall data from content variable
	var emptyStruct V

	// Execute the request and enter the results in the content variable.
	selectErr := statement.QueryRow(id).Scan(&content)
	if selectErr != nil {
		if selectErr.Error() == "sql: no rows in result set" {
			return nil, &errors.ElementNotFoundErr{Type: reflect.TypeOf(emptyStruct).String(), Id: id}
		} else {
			return nil, selectErr
		}
	}

	// Add the content to the empty struct
	json.Unmarshal([]byte(content), &emptyStruct)

	// If everything went well, we do not return any errors.
	return &emptyStruct, nil
}

func genericAddFunction[V Element](dbsql *SQLite3, id string, filledStructPointer *V) error {
	/*
	   Generic function for data insertion
	*/

	// Define the query
	strInsert := `INSERT INTO hector(id, content) VALUES(?, ?)`

	// We prepare the request corresponding to the query
	statement, err := dbsql.Database.Prepare(strInsert)
	if err != nil {
		return err
	}

	// We make sure to close the resource before the end of the function.
	defer statement.Close()

	// Convert struct to string
	bytesStruct, _ := json.Marshal(*filledStructPointer)
	strStruct := string(bytesStruct)

	// We execute the request passing the corresponding data.
	r, err := statement.Exec(id, strStruct)
	if err != nil {
		if err.Error() == "UNIQUE constraint failed: hector.id" {
			return &errors.DuplicateIDErr{Type: reflect.TypeOf(*filledStructPointer).String(), Id: id}
		} else {
			return err
		}
	}

	// We confirm that a row has been affected in the table
	if i, err := r.RowsAffected(); err != nil || i != 1 {
		return fmt.Errorf("an affected row was expected")
	}

	// If everything went well, we do not return any errors.
	return nil
}

func (dbsql *SQLite3) GetComponent(id string) (*components.Component, error) {
	/*
	   Performs a query to extract a component given its identifier
	*/

	return genericGetFunction[components.Component](dbsql, string(ComponentPrefix)+id)
}

func (dbsql *SQLite3) GetSpecification(id string) (*specifications.Specification, error) {
	/*
	   Performs a query to extract a specification given its identifier
	*/

	return genericGetFunction[specifications.Specification](dbsql, string(SpecificationPrefix)+id)
}

func (dbsql *SQLite3) GetPlanning(id string) (*[][]string, error) {
	/*
	   Performs a query to extract a planning given its identifier
	*/

	return genericGetFunction[[][]string](dbsql, string(PlanningPrefix)+id)
}

func (dbsql *SQLite3) GetDefinition(id string) (*definitions.Definition, error) {
	/*
	   Performs a query to extract a definition given its identifier
	*/

	return genericGetFunction[definitions.Definition](dbsql, string(DefinitionPrefix)+id)
}

func (dbsql *SQLite3) GetResultDefinition(id string) (*results.ResultDefinition, error) {
	/*
	   Performs a query to extract a result definition given its identifier
	*/

	return genericGetFunction[results.ResultDefinition](dbsql, string(ResultDefPrefix)+id)
}

func (dbsql *SQLite3) AddComponent(componentPointer *components.Component) error {
	/*
	   Insert component in database
	*/

	return genericAddFunction(dbsql, string(ComponentPrefix)+(*componentPointer).Id, componentPointer)
}

func (dbsql *SQLite3) AddSpecification(specificationPointer *specifications.Specification) error {
	/*
	   Insert specification in database
	*/

	return genericAddFunction(dbsql, string(SpecificationPrefix)+(*specificationPointer).Id, specificationPointer)
}

func (dbsql *SQLite3) AddPlanning(planningPointer *[][]string, specificationId string) error {
	/*
	   Insert planning in database
	*/

	return genericAddFunction(dbsql, string(PlanningPrefix)+specificationId, planningPointer)
}

func (dbsql *SQLite3) AddDefinition(definitionPointer *definitions.Definition) error {
	/*
	   Insert definition in database
	*/

	return genericAddFunction(dbsql, string(DefinitionPrefix)+(*definitionPointer).Id, definitionPointer)
}

func (dbsql *SQLite3) AddResultDefinition(resultDefinitionPointer *results.ResultDefinition) error {
	/*
	   Insert result definition in database
	*/

	return genericAddFunction(dbsql, string(ResultDefPrefix)+(*resultDefinitionPointer).Id, resultDefinitionPointer)
}

func (dbsql *SQLite3) UpdateResultJob(resultJobPointer *results.ResultJob, resultDefinitionId string) error {
	/*
		Update Result Job into Result Definition in database
	*/

	// Get Result Definition
	resultDefinitionPointer, getErr := dbsql.GetResultDefinition(resultDefinitionId)
	if getErr != nil {
		return getErr
	}

	// Search Result Job and replace/add it to the list
	idxResultJob := slices.IndexFunc((*resultDefinitionPointer).ResultJobs, func(jobRes results.ResultJob) bool { return jobRes.Id == (*resultJobPointer).Id })
	if idxResultJob == -1 {
		(*resultDefinitionPointer).ResultJobs = append((*resultDefinitionPointer).ResultJobs, *resultJobPointer)
	} else {
		(*resultDefinitionPointer).ResultJobs[idxResultJob] = *resultJobPointer
	}

	// Define the query
	strUpdate := `UPDATE hector SET content=? WHERE id=?`

	// We prepare the request corresponding to the query
	statement, err := dbsql.Database.Prepare(strUpdate)
	if err != nil {
		return err
	}

	// We make sure to close the resource before the end of the function.
	defer statement.Close()

	// Convert struct to string
	bytesStruct, _ := json.Marshal(*resultDefinitionPointer)
	strStruct := string(bytesStruct)

	// We execute the request passing the corresponding data.
	r, err := statement.Exec(strStruct, string(ResultDefPrefix)+(*resultDefinitionPointer).Id)
	if err != nil {
		return err
	}

	// We confirm that a row has been affected in the table
	if i, err := r.RowsAffected(); err != nil || i != 1 {
		return fmt.Errorf("an affected row was expected")
	}

	// If everything went well, we do not return any errors.
	return nil
}

func (dbsql *SQLite3) GetDefinitionsWithWaitings() (*[]definitions.Definition, error) {
	/*
		Returns those definitions where some of their tasks are pending execution.
	*/

	// Define the query
	strSelect := `SELECT content FROM hector WHERE`

	// We prepare the request corresponding to the query
	statement, err := dbsql.Database.Prepare(strSelect)
	if err != nil {
		return nil, err
	}

	// We make sure to close the resource before the end of the function.
	defer statement.Close()

	// We execute the request and since it will have more than one solution, we store the result in the variable rows.
	rows, err := statement.Query(ResultDefPrefix)
	if err != nil {
		return nil, err
	}

	// We make sure to close the resource before the end of the function.
	defer rows.Close()

	// We declare a slice of definitions to store the results
	definitions := []definitions.Definition{}

	// The Next method returns a bool, as long as it is true it will indicate that there is a next value to read.
	for rows.Next() {

		// We create an empty string to later store the resulting data
		var content string

		// We create an empty struct to later unmarshall data from content variable
		resDef := results.ResultDefinition{}

		// We insert the output in the content variable.
		rows.Scan(&content)

		// Add the content to the empty struct
		json.Unmarshal([]byte(content), &resDef)

		// We search if any of the tasks are pending execution
		idxSomeWaiting := slices.IndexFunc(resDef.ResultJobs, func(jobRes results.ResultJob) bool { return jobRes.Status == results.Waiting })

		// If there are any task pending execution ...
		if idxSomeWaiting != -1 {

			// We extract the corresponding definition
			defPointer, err := dbsql.GetDefinition(resDef.Id)
			if err != nil {
				return nil, fmt.Errorf("error during definition extraction %s", err.Error())
			}

			// We add the definition to the slice we declared before.
			definitions = append(definitions, *defPointer)
		}
	}

	// We return the slice of definitions
	return &definitions, nil
}
