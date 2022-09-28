package api

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/databases"
	"dag/hector/golang/module/pkg/databases/dbmock"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/executors"
	"dag/hector/golang/module/pkg/executors/execgolang"
	"dag/hector/golang/module/pkg/results"
	"dag/hector/golang/module/pkg/specifications"
	"dag/hector/golang/module/pkg/validators"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// Api is a structured type containing a field with a router, database, validator and task executor.
type Api struct {
	Router    http.Handler
	Database  databases.Database
	Validator validators.Validator
	Executor  executors.Executor
}

// Element is an interface that encompasses all the types collected in the database.
type Element interface {
	components.Component | specifications.Specification | [][]string | definitions.Definition | results.ResultDefinition
}

// Get Element function
func getElement[V Element](f func(string) (V, error), w http.ResponseWriter, r *http.Request) {
	/**
	This function is in charge of avoiding code repetition, it receives a get
	function from the database and returns the requested information based on it.
	*/

	// We collect the ID of the url
	vars := mux.Vars(r)
	id := vars["ID"]

	// We launch a query to the database
	databaseElement, err := f(id)
	if err != nil {
		log.Printf("Invalid id:", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// We write the output in the response writer
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(databaseElement)
}

// Read and Validate Element function
func readAndValidateElement[V Element](f func(*V) error, w http.ResponseWriter, r *http.Request) (V, error) {
	/**
	This function prevents code repetition, collects information from
	the request body and validates its structure.
	*/

	// Read element from body
	var element V
	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return element, fmt.Errorf("Invalid request:", err.Error())
	}
	if err := json.Unmarshal(content, &element); err != nil {
		return element, fmt.Errorf("Invalid request:", err.Error())
	}

	// Validate element scheme
	schemeErr := f(&element)
	if schemeErr != nil {
		return element, fmt.Errorf("Invalid scheme:", schemeErr.Error())
	}
	return element, nil
}

// We create a specific constructor for our problem
func NewApi() *Api {
	a := Api{}

	r := mux.NewRouter()
	r.HandleFunc("/component/submit", a.submitComponent).Methods(http.MethodPost)
	r.HandleFunc("/component/get/{ID}", a.getComponent).Methods(http.MethodGet)
	r.HandleFunc("/specification/submit", a.submitSpecification).Methods(http.MethodPost)
	r.HandleFunc("/specification/get/{ID}", a.getSpecification).Methods(http.MethodGet)
	r.HandleFunc("/topologicalSort/get/{ID}", a.getTopologicalSort).Methods(http.MethodGet)
	r.HandleFunc("/definition/execute", a.executeDefinition).Methods(http.MethodPost)
	r.HandleFunc("/definition/get/{ID}", a.getDefinition).Methods(http.MethodGet)
	r.HandleFunc("/result/get/{ID}", a.getResultDefinition).Methods(http.MethodGet)
	a.Router = r

	a.Database = dbmock.NewDBMock()
	a.Validator = *validators.NewValidator()
	a.Executor = &(execgolang.ExecGolang{})

	return &a
}

// Submit Component function
func (a *Api) submitComponent(w http.ResponseWriter, r *http.Request) {
	/**
	Function in charge of adding a new component to the database.
	*/

	// Read component from body and validate scheme
	component, err := readAndValidateElement(a.Validator.ValidateComponentStruct, w, r)
	if err != nil {
		log.Printf(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Add component to database
	databaseErr := a.Database.AddComponent(&component)
	if databaseErr != nil {
		log.Printf("Error during insertion into the database", databaseErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// Submit Specification function
func (a *Api) submitSpecification(w http.ResponseWriter, r *http.Request) {
	/**
	Function in charge of adding a new specification to the database.
	*/

	// Read specification from body and validate scheme
	specification, err := readAndValidateElement(a.Validator.ValidateSpecificationStruct, w, r)
	if err != nil {
		log.Printf(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// TODO???: Check that components are too in database ...

	// Calculate topological sort
	planning := specifications.TopologicalGroupedSort(&specification)

	// Add topological sort to database
	databasePlanningErr := a.Database.AddTopologicalSort(planning, specification.Id)
	if databasePlanningErr != nil {
		log.Printf("Error during insertion into the database", databasePlanningErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Add specification to database
	databaseSpecErr := a.Database.AddSpecification(&specification)
	if databaseSpecErr != nil {
		log.Printf("Error during insertion into the database", databaseSpecErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// Execute Definition function
func (a *Api) executeDefinition(w http.ResponseWriter, r *http.Request) {
	/**
	Function in charge of executing a definition.
	*/

	// Read definition from body and validate scheme
	definition, err := readAndValidateElement(a.Validator.ValidateDefinitionStruct, w, r)
	if err != nil {
		log.Printf(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Add definition to database
	addDefErr := a.Database.AddDefinition(&definition)

	// TODO???: Ignore duplicate id error? If there is a crash during execution and it is restarted?
	if addDefErr != nil {
		log.Printf("Error while trying to insert the definition in the database", addDefErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Get jobs in topological order
	nestedJobs, err := databases.GetJobs(&definition, &a.Database, &a.Validator)
	if err != nil {
		log.Printf("Error while trying to get jobs", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Get/Create definition result
	resultDefinition, err := a.Database.GetResultDefinition(definition.Id)
	switch err.(type) {
	case *databases.ElementNotFoundErr:
		{
			log.Printf(err.Error() + " A new document is created.")
			resultDefinition = results.ResultDefinition{
				Id:              definition.Id,
				Name:            definition.Name,
				SpecificationId: definition.SpecificationId,
				ResultJobs:      []results.ResultJob{},
			}
			err := a.Database.AddResultDefinition(&resultDefinition)
			if err != nil {
				log.Printf("Error during insertion into the database", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
	default:
		if err != nil {
			log.Printf(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	// Execute jobs
	resultDefinition.ResultJobs, err = executors.ExecuteJobs(&nestedJobs, &a.Executor, &resultDefinition, &a.Database)
	if err != nil {
		log.Printf("Error during execution", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// Functions for GET {ID} types
func (a *Api) getComponent(w http.ResponseWriter, r *http.Request) {
	getElement(a.Database.GetComponent, w, r)
}

func (a *Api) getSpecification(w http.ResponseWriter, r *http.Request) {
	getElement(a.Database.GetSpecification, w, r)
}

func (a *Api) getTopologicalSort(w http.ResponseWriter, r *http.Request) {
	getElement(a.Database.GetTopologicalSort, w, r)
}

func (a *Api) getDefinition(w http.ResponseWriter, r *http.Request) {
	getElement(a.Database.GetDefinition, w, r)
}

func (a *Api) getResultDefinition(w http.ResponseWriter, r *http.Request) {
	getElement(a.Database.GetResultDefinition, w, r)
}
