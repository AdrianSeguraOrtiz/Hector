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

	// Definimos la query
	query := `SELECT id, name, specification_id FROM resultdefinition WHERE id=?`

	// Preparamos la petición
	statement, err := dbsql.Database.Prepare(query)
	if err != nil {
		return results.ResultDefinition{}, err
	}

	// Nos aseguramos de cerrar el recurso antes de finalizar
	defer statement.Close()

	// Creamos un struct vacio
	resultDefinition := results.ResultDefinition{}

	// Ejecutamos la petición y metemos los resultados en los campos del struct
	selectErr := statement.QueryRow(id).Scan(
		&resultDefinition.Id,
		&resultDefinition.Name,
		&resultDefinition.SpecificationId,
	)
	if selectErr != nil {
		return results.ResultDefinition{}, selectErr
	}

	// Definimos la query
	query = `SELECT id, name, logs, status FROM resultjob WHERE result_definition_id=?`

	// Preparamos la petición
	statement, err = dbsql.Database.Prepare(query)
	if err != nil {
		return results.ResultDefinition{}, err
	}

	// Nos aseguramos de cerrar el recurso antes de finalizar
	defer statement.Close()

	// Ejecutamos la query
	rows, err := statement.Query(id)
	if err != nil {
		return results.ResultDefinition{}, err
	}

	// Nos aseguramos de cerrar el recurso antes de finalizar
	defer rows.Close()

	// Declaramos un slice de result jobs para almacenar los resultados
	resultJobs := []results.ResultJob{}

	// El método Next retorna un bool, mientras sea true indicará que existe un valor siguiente para leer.
	for rows.Next() {
		// Escaneamos el valor actual de la fila e insertamos la salida en los correspondientes campos del result job.
		resJob := results.ResultJob{}
		rows.Scan(
			&resJob.Id,
			&resJob.Name,
			&resJob.Logs,
			&resJob.Status,
		)
		// Añadimos el result job al slice que declaramos antes.
		resultJobs = append(resultJobs, resJob)
	}

	// Añadimos el slice de result jobs al result definition
	resultDefinition.ResultJobs = resultJobs

	// Devolvemos el result definition
	return resultDefinition, nil
}

func (dbsql *SQLite3) AddResultDefinition(resultDefinitionPointer *results.ResultDefinition) error {
	/*
	   Insert result definition in database
	*/

	// Definimos la query
	query := `INSERT INTO resultdefinition (id, name, specification_id)
            VALUES(?, ?, ?)`

	// Preparamos la petición
	statement, err := dbsql.Database.Prepare(query)
	if err != nil {
		return err
	}

	// Nos aseguramos de cerrar el recurso antes de finalizar
	defer statement.Close()

	// Ejecutamos la petición pasando los datos correspondientes.
	r, err := statement.Exec(
		(*resultDefinitionPointer).Id,
		(*resultDefinitionPointer).Name,
		(*resultDefinitionPointer).SpecificationId,
	)
	if err != nil {
		return err
	}

	// Confirmamos que una fila ha sido afectada
	if i, err := r.RowsAffected(); err != nil || i != 1 {
		return fmt.Errorf("Se esperaba una fila afectada")
	}

	// Definimos la query
	query = `INSERT INTO resultjob (id, result_definition_id, name, logs, status)
            VALUES(?, ?, ?, ?, ?)`

	// Preparamos la petición
	statement, err = dbsql.Database.Prepare(query)
	if err != nil {
		return err
	}

	// Nos aseguramos de cerrar el recurso antes de finalizar
	defer statement.Close()

	// Ejecutamos la petición pasando los datos correspondientes.
	for _, resJob := range (*resultDefinitionPointer).ResultJobs {
		r, err := statement.Exec(
			resJob.Id,
			(*resultDefinitionPointer).Id,
			resJob.Name,
			resJob.Logs,
			resJob.Status,
		)
		if err != nil {
			return err
		}

		// Confirmamos que una fila ha sido afectada
		if i, err := r.RowsAffected(); err != nil || i != 1 {
			return fmt.Errorf("Se esperaba una fila afectada")
		}
	}

	return nil
}
