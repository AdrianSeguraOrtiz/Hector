package main

import (
	"dag/hector/golang/module/pkg/api"
	"log"
	"net/http"
)

func main() {
	apiPointer, err := api.NewApi("mock", "topological_grouped", "mock")
	if err != nil {
		panic(err)
	}
	log.Fatal(http.ListenAndServe(":8080", (*apiPointer).Router))
}
