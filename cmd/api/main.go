package main

import (
	"dag/hector/golang/module/pkg/api"
	"dag/hector/golang/module/pkg/controllers"
	"dag/hector/golang/module/pkg/databases"
	"dag/hector/golang/module/pkg/databases/dbmock"
	"dag/hector/golang/module/pkg/executors"
	"dag/hector/golang/module/pkg/executors/nomad"
	"dag/hector/golang/module/pkg/filemanagers"
	"dag/hector/golang/module/pkg/filemanagers/minio"
	"dag/hector/golang/module/pkg/schedulers"
	"dag/hector/golang/module/pkg/schedulers/topologicalgrouped"
	"dag/hector/golang/module/pkg/validators"
	"log"
	"net/http"

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

	// Create Database
	var database databases.Database = dbmock.NewDBMock()

	// Create Validator
	validator := validators.NewValidator()

	// Create controller
	controller := &controllers.Controller{Executor: &executor, Scheduler: &scheduler, Database: &database, Validator: validator}

	// Create API
	api, err := api.NewApi(controller)
	if err != nil {
		panic(err)
	}

	// Raise the API
	log.Fatal(http.ListenAndServe(":8080", api.Router))

	// Set pending definitions to execute
	pendingDefinitions, err := (*controller.Database).GetDefinitionsWithWaitings()
	if err != nil {
		panic(err)
	}
	for _, def := range *pendingDefinitions {
		controller.Invoke(&def)
	}

}
