package sqlite3

import (
	"dag/hector/golang/module/pkg/results"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// We create a structured type with a pointer to the database
type SQLite3 struct {
	Database *sql.DB
}

// We create a specific constructor for our problem
func NewSQLite3() (*SQLite3, error) {
	db := SQLite3{}

	sql, err := sql.Open("sqlite3", "hector.sqlite")
	if err != nil {
		return nil, err
	}

	strCreate := `
        DROP TABLE IF EXISTS resultdefinition; 
        CREATE TABLE resultdefinition(id TEXT PRIMARY KEY, name TEXT, specification_id TEXT);
        DROP TABLE IF EXISTS resultjob; 
        CREATE TABLE resultjob(id TEXT PRIMARY KEY, result_definition_id TEXT, name TEXT, logs TEXT, status INTEGER, CONSTRAINT fk_resultdefinition FOREIGN KEY (result_definition_id) REFERENCES resultdefinition(id));
    `
	_, err = sql.Exec(strCreate)
	if err != nil {
		return nil, err
	}

	db.Database = sql

	return &db, nil
}

func (dbsql *SQLite3) GetResultDefinition(id string) (results.ResultDefinition, error) {
	/*
	   Performs a query to extract a result definition given its identifier
	*/

	// Define the query for the resultdefinition table
	selectResultDefinition := `SELECT id, name, specification_id FROM resultdefinition WHERE id=?`

	// We prepare the request corresponding to the query of resultdefinition
	statementResultDefinition, err := dbsql.Database.Prepare(selectResultDefinition)
	if err != nil {
		return results.ResultDefinition{}, err
	}

	// We make sure to close the resource before the end of the function.
	defer statementResultDefinition.Close()

	// We create an empty struct to later store the resulting data
	resultDefinition := results.ResultDefinition{}

	// Execute the request and enter the results in the struct fields.
	selectErr := statementResultDefinition.QueryRow(id).Scan(
		&resultDefinition.Id,
		&resultDefinition.Name,
		&resultDefinition.SpecificationId,
	)
	if selectErr != nil {
		return results.ResultDefinition{}, selectErr
	}

	// Define the query for the resultjob table
	selectResultJob := `SELECT id, name, logs, status FROM resultjob WHERE result_definition_id=?`

	// We prepare the request corresponding to the query of resultjob
	statementResultJob, err := dbsql.Database.Prepare(selectResultJob)
	if err != nil {
		return results.ResultDefinition{}, err
	}

	// We make sure to close the resource before the end of the function.
	defer statementResultJob.Close()

	// We execute the request and since it will have more than one solution, we store the result in the variable rows.
	rows, err := statementResultJob.Query(id)
	if err != nil {
		return results.ResultDefinition{}, err
	}

	// We make sure to close the resource before the end of the function.
	defer rows.Close()

	// We declare a slice of result jobs to store the results
	resultJobs := []results.ResultJob{}

	// The Next method returns a bool, as long as it is true it will indicate that there is a next value to read.
	for rows.Next() {

		// We create an empty resultJob to later store the data of the current row.
		resJob := results.ResultJob{}

		// We insert the output in the corresponding fields of the resultJob.
		rows.Scan(
			&resJob.Id,
			&resJob.Name,
			&resJob.Logs,
			&resJob.Status,
		)

		// We add the resultJob to the slice we declared before.
		resultJobs = append(resultJobs, resJob)
	}

	// Add the resultJobs slice to the resultDefinition
	resultDefinition.ResultJobs = resultJobs

	// We return the resultDefinition
	return resultDefinition, nil
}

func (dbsql *SQLite3) AddResultDefinition(resultDefinitionPointer *results.ResultDefinition) error {
	/*
	   Insert result definition in database
	*/

	// Define the query for the resultDefinition table
	insertResultDefinition := `INSERT INTO resultdefinition (id, name, specification_id)
            VALUES(?, ?, ?)`

	// We prepare the request corresponding to the query of resultDefinition
	statementResultDefinition, err := dbsql.Database.Prepare(insertResultDefinition)
	if err != nil {
		return err
	}

	// We make sure to close the resource before the end of the function.
	defer statementResultDefinition.Close()

	// We execute the request passing the corresponding data.
	r, err := statementResultDefinition.Exec(
		(*resultDefinitionPointer).Id,
		(*resultDefinitionPointer).Name,
		(*resultDefinitionPointer).SpecificationId,
	)
	if err != nil {
		return err
	}

	// We confirm that a row has been affected in the table of resultDefinition
	if i, err := r.RowsAffected(); err != nil || i != 1 {
		return fmt.Errorf("An affected row was expected")
	}

	// Define the query for the resultJob table
	insertResultJob := `INSERT INTO resultjob (id, result_definition_id, name, logs, status)
            VALUES(?, ?, ?, ?, ?)`

	// We prepare the request corresponding to the query of resultJob
	statementResultJob, err := dbsql.Database.Prepare(insertResultJob)
	if err != nil {
		return err
	}

	// We make sure to close the resource before the end of the function.
	defer statementResultJob.Close()

	// For each resultJob we execute the request passing the corresponding data.
	for _, resJob := range (*resultDefinitionPointer).ResultJobs {
		r, err := statementResultJob.Exec(
			resJob.Id,
			(*resultDefinitionPointer).Id,
			resJob.Name,
			resJob.Logs,
			resJob.Status,
		)
		if err != nil {
			return err
		}

		// In each iteration we confirm that a row has been affected in the resultJob table.
		if i, err := r.RowsAffected(); err != nil || i != 1 {
			return fmt.Errorf("An affected row was expected")
		}
	}

	// If everything went well, we do not return any errors.
	return nil
}
