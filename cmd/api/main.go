package main

import (
	"log"
	"net/http"
	"dag/hector/golang/module/api"
)

func main() {
	a := api.NewApi()
	log.Fatal(http.ListenAndServe(":8080", a.router))
}
