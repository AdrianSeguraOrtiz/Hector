package api

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"golang.org/x/exp/slices"
)

database := dbmock.DBMock{}

// api es un tipo estructurado que contiene un campo con un router
type api struct {
	router http.Handler
}

// Creamos un constructor específico para nuestro problema
func NewApi() *api {
	a := api{}

	r := mux.NewRouter()
	r.HandleFunc("/workflow/submit", a.submitWorkflow).Methods(http.MethodPost)
	r.HandleFunc("/execution/execute", a.executeExecution).Methods(http.MethodPost)
	r.HandleFunc("/execution/get/{ID}", a.getExecution).Methods(http.MethodGet)

	a.router = r
	return &a
}

func (a *api) submitWorkflow(w http.ResponseWriter, r *http.Request) {
	// Read workflow from body
	var workflow workflows.Workflow
	err := json.NewDecoder(r.Body).Decode(&w)
	pkg.Check(err)

	// Initialize the validator
	validator := validators.Validator{}

	// Validate workflow
	workflowErr := validator.ValidateWorkflowStruct(&workflow)
	if workflowErr != nil {
		fmt.Println(workflowErr)
	}

	// Add workflow to database
	database.AddWorkflow(&workflow)
}

func (a *api) executeExecution(w http.ResponseWriter, r *http.Request) {
	// Por implementar
	err := json.NewDecoder(r.Body)
	pkg.Check(err)
}

func (a *api) getExecution(w http.ResponseWriter, r *http.Request) {
	// Recogemos el ID de la url
	vars := mux.Vars(r)
	id := vars["ID"]

	// Simulamos la llamada a una base de datos
	resultExecution := database.GetResultExecution(id)

	// Escribimos la salida en el response writer
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resultExecution)
}
