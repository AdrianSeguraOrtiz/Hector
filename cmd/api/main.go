package main

import (
	"dag/hector/golang/module/pkg/api"
	"dag/hector/golang/module/pkg/controllers"
	"dag/hector/golang/module/pkg/datastores"
	"dag/hector/golang/module/pkg/datastores/sqlite3"
	"dag/hector/golang/module/pkg/executors"
	"dag/hector/golang/module/pkg/executors/nomad"
	"dag/hector/golang/module/pkg/filemanagers"
	"dag/hector/golang/module/pkg/filemanagers/minio"
	"dag/hector/golang/module/pkg/schedulers"
	"dag/hector/golang/module/pkg/schedulers/topologicalgrouped"
	"dag/hector/golang/module/pkg/validators"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	// Read environment variables
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}

	// Create FileManager
	var fileManager filemanagers.FileManager
	fileManager, err = minio.NewMinio()
	if err != nil {
		panic(err)
	}

	// Create Executor
	var executor executors.Executor = nomad.NewNomad(&fileManager)

	// Create Scheduler
	var scheduler schedulers.Scheduler = topologicalgrouped.NewTopologicalGrouped()

	// Create Datastore
	var datastore datastores.Datastore
	var err error
	datastore, err = sqlite3.NewSQLite3()
	if err != nil {
		panic(err)
	}

	// Create Validator
	validator := validators.NewValidator()

	// Create controller
	controller := &controllers.Controller{Executor: &executor, Scheduler: &scheduler, Datastore: &datastore, Validator: validator}

	// Create API
	api, err := api.NewApi(controller)
	if err != nil {
		panic(err)
	}

	// Raise the API
	host := os.Getenv("API_HOST")
	port := os.Getenv("API_PORT")
	log.Fatal(http.ListenAndServe(host+":"+port, api.Router))

	// Set pending definitions to execute
	pendingDefinitions, err := (*controller.Datastore).GetDefinitionsWithWaitings()
	if err != nil {
		panic(err)
	}
	for _, def := range *pendingDefinitions {
		controller.Invoke(&def)
	}

}
