package api

import (
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/controllers"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/results"
	"dag/hector/golang/module/pkg/specifications"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rs/xid"
)

// Api is a structured type containing a field with a router, datastore, validator and task executor.
type Api struct {
	Router     http.Handler
	Controller *controllers.Controller
}

// Element is an interface that encompasses all the types collected in the datastore.
type Element interface {
	components.Component | specifications.Specification | [][]string | definitions.Definition | results.ResultDefinition
}

// getElement function implements a generic procedure that is in charge of answering
// requests that ask for information about a certain element in the datastore. To do so,
// it requires the function in charge of performing the extraction from the datastore.
// Finally, it records the result in the body of the response. It takes as input the
// request, the get function that communicates with the datastore and the variable type
// ResponseWriter where the output is registered.
func getElement[V Element](f func(string) (*V, error), w http.ResponseWriter, r *http.Request) {

	// We collect the ID of the url
	vars := mux.Vars(r)
	id := vars["ID"]

	// We launch a query to the datastore
	datastoreElement, err := f(id)
	if err != nil {
		log.Printf("Invalid id: %s", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// We write the output in the response writer
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(*datastoreElement)
}

// readAndValidateElement function implements a generic procedure that reads the content of
// an element in the request body and validates its structure. To do so, it requires the
// function in charge of performing such validation. It takes as input the request and a
// validation function. It provides in the output the element read and an error type variable
// notifying of any problem.
func readAndValidateElement[V Element](f func(*V) error, r *http.Request) (V, error) {

	// Read element from body
	var element V
	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return element, fmt.Errorf("invalid request: %s", err.Error())
	}
	if err := json.Unmarshal(content, &element); err != nil {
		return element, fmt.Errorf("invalid request: %s", err.Error())
	}

	// Validate element scheme
	schemeErr := f(&element)
	if schemeErr != nil {
		return element, fmt.Errorf("invalid scheme: %s", schemeErr.Error())
	}
	return element, nil
}

// NewApi function creates a new instance of type Api. It takes as input a controller.
// It returns the pointer to the new instance of the api and an error variable to
// report any problems.
func NewApi(controller *controllers.Controller) (*Api, error) {
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

	a.Controller = controller

	return &a, nil
}

// submitComponent function is responsible for extracting the component element from
// the request body and inserting it into the datastore. It takes as input the request
// and the variable type ResponseWriter where the result of the operation is notified.
func (a *Api) submitComponent(w http.ResponseWriter, r *http.Request) {

	// Read component from body and validate scheme
	component, err := readAndValidateElement(a.Controller.Validator.ValidateComponentStruct, r)
	if err != nil {
		log.Print(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Add component to datastore
	datastoreErr := (*a.Controller.Datastore).AddComponent(&component)
	if datastoreErr != nil {
		log.Printf("error during insertion into the datastore %s", datastoreErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// submitSpecification function is responsible for extracting the specification element from
// the request body and inserting it into the datastore. It takes as input the request
// and the variable type ResponseWriter where the result of the operation is notified.
func (a *Api) submitSpecification(w http.ResponseWriter, r *http.Request) {

	// Read specification from body and validate scheme
	specification, err := readAndValidateElement(a.Controller.Validator.ValidateSpecificationStruct, r)
	if err != nil {
		log.Print(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// TODO???: Check that components are too in datastore ...

	// Calculate topological sort
	planning, err := (*a.Controller.Scheduler).Plan(&specification)
	if err != nil {
		log.Printf("error during planning calculation %s", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Add topological sort to datastore
	datastorePlanningErr := (*a.Controller.Datastore).AddPlanning(&planning, specification.Id)
	if datastorePlanningErr != nil {
		log.Printf("error during insertion into the datastore %s", datastorePlanningErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Add specification to datastore
	datastoreSpecErr := (*a.Controller.Datastore).AddSpecification(&specification)
	if datastoreSpecErr != nil {
		log.Printf("error during insertion into the datastore %s", datastoreSpecErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// executeDefinition function extracts the Definition element from the request body and sends
// it to the controller for execution. Finally, it records the status of the operation in the
// variable type ResponseWriter. It takes as input the request and the variable type ResponseWriter.
func (a *Api) executeDefinition(w http.ResponseWriter, r *http.Request) {

	// Read definition from body and validate scheme
	definition, err := readAndValidateElement(a.Controller.Validator.ValidateDefinitionStruct, r)
	if err != nil {
		log.Print(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Generate random id
	definition.Id = xid.New().String()

	// Add definition to datastore
	addDefErr := (*a.Controller.Datastore).AddDefinition(&definition)
	if addDefErr != nil {
		log.Printf("error while trying to insert the definition in the datastore %s", addDefErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, invErr := a.Controller.Invoke(&definition)
	if invErr != nil {
		log.Printf("error during invocation of the definition %s", invErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// If everything has gone well, we set the status to satisfactory and return the execution id
	fmt.Println(definition.Id)
	w.WriteHeader(http.StatusOK)
}

// getComponent function is responsible for resolving requests for information about a particular
// Component element. To do so, it extracts the identifier from the body of the request and records
// the result in the ResponseWriter type variable. It takes as input the request and the
// ResponseWriter variable.
func (a *Api) getComponent(w http.ResponseWriter, r *http.Request) {
	getElement((*a.Controller.Datastore).GetComponent, w, r)
}

// getSpecification function is responsible for resolving requests for information about a particular
// Specification element. To do so, it extracts the identifier from the body of the request and records
// the result in the ResponseWriter type variable. It takes as input the request and the
// ResponseWriter variable.
func (a *Api) getSpecification(w http.ResponseWriter, r *http.Request) {
	getElement((*a.Controller.Datastore).GetSpecification, w, r)
}

// getTopologicalSort function is responsible for resolving requests for information about a particular
// Planning element. To do so, it extracts the identifier from the body of the request and records
// the result in the ResponseWriter type variable. It takes as input the request and the
// ResponseWriter variable.
func (a *Api) getTopologicalSort(w http.ResponseWriter, r *http.Request) {
	getElement((*a.Controller.Datastore).GetPlanning, w, r)
}

// getDefinition function is responsible for resolving requests for information about a particular
// Definition element. To do so, it extracts the identifier from the body of the request and records
// the result in the ResponseWriter type variable. It takes as input the request and the
// ResponseWriter variable.
func (a *Api) getDefinition(w http.ResponseWriter, r *http.Request) {
	getElement((*a.Controller.Datastore).GetDefinition, w, r)
}

// getResultDefinition function is responsible for resolving requests for information about a particular
// ResultDefinition element. To do so, it extracts the identifier from the body of the request and records
// the result in the ResponseWriter type variable. It takes as input the request and the
// ResponseWriter variable.
func (a *Api) getResultDefinition(w http.ResponseWriter, r *http.Request) {
	getElement((*a.Controller.Datastore).GetResultDefinition, w, r)
}
