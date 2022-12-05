package controllers

import (
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/databases"
	"dag/hector/golang/module/pkg/databases/dbmock"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"
	"dag/hector/golang/module/pkg/specifications"
	"dag/hector/golang/module/pkg/validators"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
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

	// Create Database
	var database databases.Database = dbmock.NewDBMock()

	// Insert test specification
	database.AddSpecification(&testSpecification)

	// Insert test planning
	database.AddPlanning(&testPlanning, testSpecification.Id)

	// Create Validator
	validator := validators.NewValidator()

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			specification, planning, err := getAndCheckSpecPlanning(tt.definition, &database, validator)
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

	// Create Database
	var database databases.Database = dbmock.NewDBMock()

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
	database.AddComponent(&testComponent)

	// Insert test specification
	database.AddSpecification(&testSpecification)

	// Insert test planning
	database.AddPlanning(&testPlanning, testSpecification.Id)

	// Create Validator
	validator := validators.NewValidator()

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			job, err := getAndCheckJob(tt.definition, "A", &testSpecification, &database, validator)
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

	// Create Database
	var database databases.Database = dbmock.NewDBMock()

	// Add executed result definition to the database
	database.AddResultDefinition(&executedResultDefinition)

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			resultDefinition, _ := getOrDefaultResultDefinition(tt.definition, &database, tt.nestedJobs)

			equal, message := pkg.DeepValueEqual(*resultDefinition, *tt.resultDefinition, true)
			if !equal {
				t.Error("The result definition obtained has not been as expected. " + message)
			}
		})
	}
}
