package controllers

import (
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/datastores"
	"dag/hector/golang/module/pkg/datastores/dbmock"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/executors"
	"dag/hector/golang/module/pkg/executors/execmock"
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"dag/hector/golang/module/pkg/specifications"
	"dag/hector/golang/module/pkg/validators"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func TestGetAndCheckSpecPlanning(t *testing.T) {

	// Declare test specification
	testSpecification := specifications.Specification{
		Id: "Spec-ID",
		Spec: specifications.Spec{
			Dag: specifications.Dag{
				Tasks: []specifications.SpecificationTask{
					{
						Name:      "A",
						Component: "Comp1-ID",
					},
					{
						Name:         "B",
						Dependencies: []string{"A"},
						Component:    "Comp2-ID",
					},
					{
						Name:         "C",
						Dependencies: []string{"A"},
						Component:    "Comp3-ID",
					},
					{
						Name:         "D",
						Dependencies: []string{"B", "C"},
						Component:    "Comp4-ID",
					},
				},
			},
		},
	}

	// Declare test planning
	testPlanning := [][]string{{"A"}, {"B", "C"}, {"D"}}

	// Declare test definitions
	goodDefinition := definitions.Definition{
		SpecificationId: "Spec-ID",
		Data: definitions.Data{
			Tasks: []definitions.DefinitionTask{
				{
					Name: "A",
				},
				{
					Name: "B",
				},
				{
					Name: "C",
				},
				{
					Name: "D",
				},
			},
		},
	}

	strGoodDefinition, _ := json.Marshal(goodDefinition)

	badDefinition1 := definitions.Definition{}
	json.Unmarshal(strGoodDefinition, &badDefinition1)
	badDefinition1.SpecificationId = "Bad-Spec-ID"

	badDefinition2 := definitions.Definition{}
	json.Unmarshal(strGoodDefinition, &badDefinition2)
	badDefinition2.Data.Tasks[0].Name = "Bad-Name-A"

	// Classic tests variable
	var tests = []struct {
		definition    *definitions.Definition
		specification *specifications.Specification
		planning      *[][]string
		err           string
	}{
		{
			definition:    &goodDefinition,
			specification: &testSpecification,
			planning:      &testPlanning,
			err:           "",
		},
		{
			definition:    &badDefinition1,
			specification: nil,
			planning:      nil,
			err:           "specifications.Specification with id Bad-Spec-ID not found in database.",
		},
		{
			definition:    &badDefinition2,
			specification: nil,
			planning:      nil,
			err:           "task A is required in the selected specification but is not present in the definition file",
		},
	}

	// Create Datastore
	var datastore datastores.Datastore = dbmock.NewDBMock()

	// Insert test specification
	datastore.AddSpecification(&testSpecification)

	// Insert test planning
	datastore.AddPlanning(&testPlanning, testSpecification.Id)

	// Create Validator
	validator := validators.NewValidator()

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			specification, planning, err := getAndCheckSpecPlanning(tt.definition, &datastore, validator)
			if err == nil {
				err = fmt.Errorf("")
			}

			if tt.err != err.Error() {
				t.Error("The error obtained was not as expected. Got " + err.Error() + " but want " + tt.err)
			} else {
				if tt.specification != nil && specification != nil {
					specEqual, specMessage := pkg.DeepValueEqual(*specification, *tt.specification, true)
					if !specEqual {
						t.Error("The specification obtained has not been as expected. " + specMessage)
					}
				}
				if tt.planning != nil && planning != nil {
					planningEqual, planningMessage := pkg.DeepValueEqual(*planning, *tt.planning, true)
					if !planningEqual {
						t.Error("The planning obtained has not been as expected. " + planningMessage)
					}
				}
			}
		})
	}

}

func TestGetAndCheckJob(t *testing.T) {

	// Declare test job
	testJob := jobs.Job{
		Name:  "A",
		Image: "image/name",
		Arguments: []definitions.Parameter{
			{
				Name:  "input_1",
				Value: "Input string value",
			},
			{
				Name:  "input_2",
				Value: 22,
			},
			{
				Name:  "output_1",
				Value: "path/to/output_file.csv",
			},
		},
	}

	// Declare test definitions
	goodDefinition := definitions.Definition{
		SpecificationId: "Spec-ID",
		Data: definitions.Data{
			Tasks: []definitions.DefinitionTask{
				{
					Name: "A",
					Inputs: []definitions.Parameter{
						{
							Name:  "input_1",
							Value: "Input string value",
						},
						{
							Name:  "input_2",
							Value: 22,
						},
					},
					Outputs: []definitions.Parameter{
						{
							Name:  "output_1",
							Value: "path/to/output_file.csv",
						},
					},
				},
			},
		},
	}

	strGoodDefinition, _ := json.Marshal(goodDefinition)

	badDefinition1 := definitions.Definition{}
	json.Unmarshal(strGoodDefinition, &badDefinition1)
	badDefinition1.Data.Tasks[0].Inputs[1].Value = "bad type"

	badDefinition2 := definitions.Definition{}
	json.Unmarshal(strGoodDefinition, &badDefinition2)
	badDefinition2.Data.Tasks[0].Inputs[1].Value = goodDefinition.Data.Tasks[0].Inputs[1].Value.(int)
	badDefinition2.Data.Tasks[0].Outputs[0].Name = "bad name"

	// Classic tests variable
	var tests = []struct {
		definition *definitions.Definition
		job        *jobs.Job
		err        string
	}{
		{
			definition: &goodDefinition,
			job:        &testJob,
			err:        "",
		},
		{
			definition: &badDefinition1,
			job:        nil,
			err:        "parameter input_2 has an invalid value in the definition file",
		},
		{
			definition: &badDefinition2,
			job:        nil,
			err:        "parameter output_1 is required but is not present in the definition file",
		},
	}

	// Create Datastoree
	var datastore datastores.Datastore = dbmock.NewDBMock()

	// Declare test component
	testComponent := components.Component{
		Id: "Comp1-ID",
		Inputs: []components.Put{
			{
				Name: "input_1",
				Type: "string",
			},
			{
				Name: "input_2",
				Type: "int",
			},
		},
		Outputs: []components.Put{
			{
				Name: "output_1",
				Type: "string",
			},
		},
		ContainerDockerfile: "components/component_file.dockerfile",
		ContainerImage:      "image/name",
	}

	// Declare test specification
	testSpecification := specifications.Specification{
		Id: "Spec-ID",
		Spec: specifications.Spec{
			Dag: specifications.Dag{
				Tasks: []specifications.SpecificationTask{
					{
						Name:      "A",
						Component: "Comp1-ID",
					},
				},
			},
		},
	}

	// Declare test planning
	testPlanning := [][]string{{"A"}}

	// Insert test components
	datastore.AddComponent(&testComponent)

	// Insert test specification
	datastore.AddSpecification(&testSpecification)

	// Insert test planning
	datastore.AddPlanning(&testPlanning, testSpecification.Id)

	// Create Validator
	validator := validators.NewValidator()

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			job, err := getAndCheckJob(tt.definition, "A", &testSpecification, &datastore, validator)
			if err == nil {
				err = fmt.Errorf("")
			}

			if tt.err != err.Error() {
				t.Error("The error obtained was not as expected. Got " + err.Error() + " but want " + tt.err)
			} else if tt.job != nil && job != nil {
				job.Id = tt.job.Id
				equal, message := pkg.DeepValueEqual(*job, *tt.job, true)
				if !equal {
					t.Error("The specification obtained has not been as expected. " + message)
				}
			}
		})
	}

}

func TestGetOrDefaultResultDefinition(t *testing.T) {

	// Declare test definitions
	executedDefinition := definitions.Definition{
		Id:              "Executed-Def-Id",
		Name:            "Def-Name",
		SpecificationId: "Spec-ID",
		Data: definitions.Data{
			Tasks: []definitions.DefinitionTask{
				{
					Name: "A",
				},
			},
		},
	}

	notExecutedDefinition := definitions.Definition{
		Id:              "Not-Executed-Def-Id",
		Name:            "Def-Name",
		SpecificationId: "Spec-ID",
		Data: definitions.Data{
			Tasks: []definitions.DefinitionTask{
				{
					Name: "A",
				},
			},
		},
	}

	// Declare Nested Jobs
	nestedJobs := [][]jobs.Job{{jobs.Job{Name: "A"}}}

	// Declare result definitions
	executedResultDefinition := results.ResultDefinition{
		Id:              executedDefinition.Id,
		Name:            executedDefinition.Name,
		SpecificationId: executedDefinition.SpecificationId,
		ResultJobs: []results.ResultJob{
			{
				Name:   "A",
				Status: results.Done,
			},
		},
	}

	notExecutedResultDefinition := results.ResultDefinition{
		Id:              notExecutedDefinition.Id,
		Name:            notExecutedDefinition.Name,
		SpecificationId: notExecutedDefinition.SpecificationId,
		ResultJobs: []results.ResultJob{
			{
				Name:   "A",
				Status: results.Waiting,
			},
		},
	}

	// Classic tests variable
	var tests = []struct {
		definition       *definitions.Definition
		nestedJobs       *[][]jobs.Job
		resultDefinition *results.ResultDefinition
	}{
		{
			definition:       &executedDefinition,
			nestedJobs:       &nestedJobs,
			resultDefinition: &executedResultDefinition,
		},
		{
			definition:       &notExecutedDefinition,
			nestedJobs:       &nestedJobs,
			resultDefinition: &notExecutedResultDefinition,
		},
	}

	// Create Datastore
	var datastore datastores.Datastore = dbmock.NewDBMock()

	// Add executed result definition to the datastore
	datastore.AddResultDefinition(&executedResultDefinition)

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			resultDefinition, _ := getOrDefaultResultDefinition(tt.definition, &datastore, tt.nestedJobs)

			equal, message := pkg.DeepValueEqual(*resultDefinition, *tt.resultDefinition, true)
			if !equal {
				t.Error("The result definition obtained has not been as expected. " + message)
			}
		})
	}
}

func TestCheckJobExecutionRequirements(t *testing.T) {

	// Declare job results (local storage)
	jobResults := map[string]results.ResultJob{
		"NameJP1": {
			Id:     "JP1",
			Status: results.Done,
		},
		"NameJ1": {
			Id:     "J1",
			Status: results.Waiting,
		},
		"NameJP2": {
			Id:     "JP2",
			Status: results.Error,
		},
		"NameJ2": {
			Id:     "J2",
			Status: results.Waiting,
		},
		"NameJP3": {
			Id:     "JP3",
			Status: results.Cancelled,
		},
		"NameJ3": {
			Id:     "J3",
			Status: results.Waiting,
		},
		"NameJ4": {
			Id:     "J4",
			Status: results.Waiting,
		},
		"NameJ5": {
			Id:     "J5",
			Status: results.Done,
		},
		"NameJ6": {
			Id:     "J6",
			Status: results.Error,
		},
		"NameJ7": {
			Id:     "J7",
			Status: results.Cancelled,
		},
	}

	// Declare result definitions
	resultDefinition := results.ResultDefinition{
		Id:         "RD-ID",
		ResultJobs: maps.Values(jobResults),
	}

	// Classic tests variable
	var tests = []struct {
		job   *jobs.Job
		valid bool
	}{
		{
			job: &jobs.Job{
				Id:           "J1",
				Name:         "NameJ1",
				Dependencies: []string{"NameJP1"},
			},
			valid: true,
		},
		{
			job: &jobs.Job{
				Id:           "J2",
				Name:         "NameJ2",
				Dependencies: []string{"NameJP2"},
			},
			valid: false,
		},
		{
			job: &jobs.Job{
				Id:           "J3",
				Name:         "NameJ3",
				Dependencies: []string{"NameJP3"},
			},
			valid: false,
		},
		{
			job:   &jobs.Job{Id: "J4", Name: "NameJ4"},
			valid: true,
		},
		{
			job:   &jobs.Job{Id: "J5", Name: "NameJ5"},
			valid: false,
		},
		{
			job:   &jobs.Job{Id: "J6", Name: "NameJ6"},
			valid: false,
		},
		{
			job:   &jobs.Job{Id: "J7", Name: "NameJ7"},
			valid: false,
		},
	}

	// Create Datastore
	var datastore datastores.Datastore = dbmock.NewDBMock()

	// Add result definition to the datastore
	datastore.AddResultDefinition(&resultDefinition)

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			validForExecution, err := checkJobExecutionRequirements(tt.job, &jobResults, &datastore, resultDefinition.Id)

			if err != nil {
				t.Error("Unexpected error detected: " + err.Error())
			} else if validForExecution != tt.valid {
				t.Error("The function has provided an unexpected value. Got " + strconv.FormatBool(validForExecution) + " but want " + strconv.FormatBool(tt.valid))
			} else if len(tt.job.Dependencies) > 0 && jobResults[tt.job.Dependencies[0]].Status != results.Done {
				if status := jobResults[tt.job.Name].Status; status != results.Cancelled {
					t.Error("The status recorded in the local storage is not correct. Cancelled was expected but obtained " + fmt.Sprintf("%v", status))
				}
				resDef, _ := datastore.GetResultDefinition(resultDefinition.Id)
				idx := slices.IndexFunc(resDef.ResultJobs, func(rj results.ResultJob) bool { return rj.Id == tt.job.Id })
				if status := resDef.ResultJobs[idx].Status; status != results.Cancelled {
					t.Error("The status recorded in the remote storage is not correct. Cancelled was expected but obtained " + fmt.Sprintf("%v", status))
				}
			}
		})
	}
}

func TestRunAndUpdateStatus(t *testing.T) {

	// Declare job
	job := jobs.Job{
		Id:   "J1",
		Name: "NameJ1",
	}

	// Declare job results (local storage)
	jobResults := map[string]results.ResultJob{
		"NameJ1": {
			Id:     "J1",
			Status: results.Waiting,
		},
	}

	// Declare result definitions
	resultDefinition := results.ResultDefinition{
		Id:         "RD-ID",
		ResultJobs: maps.Values(jobResults),
	}

	// Create Executor
	var executor executors.Executor = execmock.NewExecMock()

	// We created an access control system to prevent co-occurrence into goroutines
	mutex := &sync.RWMutex{}

	// Create Datastore
	var datastore datastores.Datastore = dbmock.NewDBMock()

	// Add result definition to the datastore
	datastore.AddResultDefinition(&resultDefinition)

	t.Run("test", func(t *testing.T) {
		err := runAndUpdateStatus(&executor, &job, mutex, &jobResults, &datastore, resultDefinition.Id)

		if err != nil {
			t.Error("Unexpected error detected: " + err.Error())
		} else if jobResults[job.Name].Status == results.Waiting {
			t.Error("The status registered in the local storage has not been updated")
		} else if rd, _ := datastore.GetResultDefinition(resultDefinition.Id); rd.ResultJobs[0].Status == results.Waiting {
			t.Error("The status registered in the remote storage has not been updated")
		}
	})
}
