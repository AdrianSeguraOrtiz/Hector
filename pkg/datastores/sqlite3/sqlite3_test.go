package sqlite3

import (
	"dag/hector/golang/module/pkg/results"
	"fmt"
	"strconv"
	"testing"
)

// TODO: I leave the rest of the tests pending after the review.

func TestAddResultDefinition(t *testing.T) {
	resultDefinition := results.ResultDefinition{
		Id:              "Result-Definition-Id",
		Name:            "Result Definition Name",
		SpecificationId: "Specification Id",
		ResultJobs: []results.ResultJob{
			{
				Id:     "Result-Job-Id",
				Name:   "Result Job Name",
				Logs:   "All right",
				Status: results.Done,
			},
		},
	}

	var tests = []struct {
		resDef *results.ResultDefinition
		want   string
	}{
		{&resultDefinition, ""},
		{&resultDefinition, "A results.ResultDefinition with id resdef-Result-Definition-Id is already stored in the database."},
	}

	sqlite3, _ := NewSQLite3()

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			err := sqlite3.AddResultDefinition(tt.resDef)

			if err == nil {
				err = fmt.Errorf("")
			}
			if err.Error() != tt.want {
				t.Error("got ", err, ", want ", tt.want)
			}
		})
	}
}

func TestGetResultDefinition(t *testing.T) {
	resultDefinition := results.ResultDefinition{
		Id:              "Result-Definition-Id",
		Name:            "Result Definition Name",
		SpecificationId: "Specification-Id",
		ResultJobs: []results.ResultJob{
			{
				Id:     "Result-Job-Id",
				Name:   "Result Job Name",
				Logs:   "All right",
				Status: results.Done,
			},
		},
	}

	sqlite3, _ := NewSQLite3()
	sqlite3.AddResultDefinition(&resultDefinition)

	var tests = []struct {
		id   string
		want string
	}{
		{"Result-Definition-Id", ""},
		{"Bad-Id", "results.ResultDefinition with id resdef-Bad-Id not found in database."},
	}

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			resDefPointer, err := sqlite3.GetResultDefinition(tt.id)

			if err == nil {
				if (*resDefPointer).Id != resultDefinition.Id {
					t.Error("The result job returned by the GetResultDefinition() function is not the correct one.")
				}
				err = fmt.Errorf("")
			}
			if err.Error() != tt.want {
				t.Error("got ", err, ", want ", tt.want)
			}
		})
	}
}
