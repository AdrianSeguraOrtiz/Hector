package main

import (
	"dag/hector/golang/module/pkg/api"
	"log"
	"net/http"
)

func main() {
	apiPointer := api.NewApi()
	log.Fatal(http.ListenAndServe(":8080", (*apiPointer).Router))
}
