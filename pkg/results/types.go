package results

import "encoding/json"

type Status int64

const (
	Waiting Status = iota
	Done
	Error
	Cancelled
)

type ResultJob struct {
	Id     string
	Name   string
	Logs   string
	Status Status
}

type ResultDefinition struct {
	Id              string
	Name            string
	SpecificationId string
	ResultJobs      []ResultJob
}

func (rdef *ResultDefinition) String() string {
	s, _ := json.MarshalIndent(rdef, "", "  ")
	return string(s)
}
