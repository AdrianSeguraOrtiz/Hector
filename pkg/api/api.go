package api

import (
	"dag/hector/golang/module/pkg/databases"
	"dag/hector/golang/module/pkg/databases/dbmock"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/specifications"
	"dag/hector/golang/module/pkg/validators"
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// Api es un tipo estructurado que contiene un campo con un router y la base de datos
type Api struct {
	Router   http.Handler
	Database databases.Database
}

func getThroughApi(fn func(string) [V int | float64](V, error), w http.ResponseWriter, r *http.Request) {
	// Recogemos el ID de la url
	vars := mux.Vars(r)
	id := vars["ID"]

	// Simulamos la llamada a una base de datos
	databaseElement, err := fn(id)
	if err != nil {
		log.Printf("Invalid id:", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Escribimos la salida en el response writer
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(databaseElement)
}

// Creamos un constructor específico para nuestro problema
func NewApi() *Api {
	a := Api{}

	r := mux.NewRouter()
	r.HandleFunc("/specification/submit", a.submitSpecification).Methods(http.MethodPost)
	r.HandleFunc("/definition/execute", a.executeDefinition).Methods(http.MethodPost)
	r.HandleFunc("/result/get/{ID}", a.getResultDefinition).Methods(http.MethodGet)
	a.Router = r

	a.Database = &(dbmock.DBMock{})

	return &a
}

func (a *Api) submitSpecification(w http.ResponseWriter, r *http.Request) {
	// Read specification from body
	var specification specifications.Specification
	err := specification.FromRequest(r)
	if err != nil {
		log.Printf("Invalid request:", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Initialize the validator
	validatorPointer := validators.NewValidator()

	// Validate specification
	specificationErr := validatorPointer.ValidateSpecificationStruct(&specification)
	if specificationErr != nil {
		log.Printf("Invalid specification scheme:", specificationErr.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// TODO: Check that components are too in database ...

	// Add specification to database
	a.Database.AddSpecification(&specification)

	w.WriteHeader(http.StatusOK)
}

func (a *Api) executeDefinition(w http.ResponseWriter, r *http.Request) {
	// Por implementar
	var definition definitions.Definition
	err := json.NewDecoder(r.Body).Decode(&definition)
	log.Printf(err.Error())
}

func (a *Api) getResultDefinition(w http.ResponseWriter, r *http.Request) {
	getThroughApi(a.Database.GetResultDefinition, w, r)
}
