package main

import (
	"flag"

	"hector/pkg/controller"
	"hector/pkg/workflow"
)

func main() {
	var workflowSpec string

	flag.StringVar(&workflowSpec, "wf", "", "Workflow definition file")
	flag.Parse()

	wf := &workflow.WorkflowSpec{}
	if err := wf.FromFile(workflowSpec); err != nil {
		panic(err)
	}

	ctrl := controller.NewController()
	results, _ := ctrl.Invoke(wf)
}
