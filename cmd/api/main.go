package main

import (
	"dag/hector/golang/module/pkg/api"
	"dag/hector/golang/module/pkg/controllers"
	"log"
	"net/http"
)

func main() {
	// Create controller
	controller, err := controllers.NewController("mock", "topological_grouped", "mock")
	if err != nil {
		panic(err)
	}

	// Create API
	apiPointer, err := api.NewApi(controller)
	if err != nil {
		panic(err)
	}

	// Raise the API
	log.Fatal(http.ListenAndServe(":8080", (*apiPointer).Router))

	// Set pending definitions to execute
	pendingDefinitions, err := (*controller.Database).GetDefinitionsWithWaitings()
	if err != nil {
		panic(err)
	}
	for _, def := range pendingDefinitions {
		controller.Invoke(&def)
	}

}
