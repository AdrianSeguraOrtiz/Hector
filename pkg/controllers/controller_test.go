package controllers

import (
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/databases"
	"dag/hector/golang/module/pkg/databases/dbmock"
	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/specifications"
	"dag/hector/golang/module/pkg/validators"
	"strconv"
	"testing"
)

func TestGetAndCheckSpecPlanning(t *testing.T) {
	var tests = []struct {
		definition *definitions.Definition
		err        string
	}{
		{
			definition: &definitions.Definition{
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
			},
			err: "",
		},
		{
			definition: &definitions.Definition{
				SpecificationId: "Bad-Spec-ID",
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
			},
			err: "specifications.Specification with id Bad-Spec-ID not found in database.",
		},
		{
			definition: &definitions.Definition{
				SpecificationId: "Spec-ID",
				Data: definitions.Data{
					Tasks: []definitions.DefinitionTask{
						{
							Name: "Bad-Name-A",
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
			},
			err: "task A is required in the selected specification but is not present in the definition file",
		},
	}

	// Create Database
	var database databases.Database = dbmock.NewDBMock()

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

			if err != nil {
				if tt.err != err.Error() {
					t.Error("The error obtained was not as expected. Got " + err.Error() + " but want " + tt.err)
				}
			} else {
				if tt.err != "" {
					t.Error("An error was expected but it has not been released. Expected: " + tt.err)
				} else {
					specEqual, specMessage := pkg.DeepValueEqual(*specification, testSpecification, true)
					planningEqual, planningMessage := pkg.DeepValueEqual(*planning, testPlanning, true)

					if !specEqual {
						t.Error("The specification obtained has not been as expected. " + specMessage)
					} else if !planningEqual {
						t.Error("The planning obtained has not been as expected. " + planningMessage)
					}
				}
			}
		})
	}

}
