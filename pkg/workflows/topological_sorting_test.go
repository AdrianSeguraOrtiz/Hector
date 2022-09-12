package workflows

import (
    "testing"
	"reflect"
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/workflows"
	"encoding/json"
	"path/filepath"
	"strings"
)

func TestTopologicalSort(t *testing.T) {
	var tests = []struct {
		workflowFile string
		want []string
	}{
		{"./../../data/hector/workflow_tests/workflow_test_1.json", []string{"A", "C", "B", "D"}},
		{"./../../data/hector/workflow_tests/workflow_test_2.json", []string{"5", "4", "0", "2", "3", "1"}}, // Based on https://www.geeksforgeeks.org/topological-sorting/ DAG
		{"./../../data/hector/workflow_tests/workflow_test_3.json", []string{"7", "5", "11", "2", "3", "8", "9", "10"}}, // Based on https://upload.wikimedia.org/wikipedia/commons/0/08/Directed_acyclic_graph.png
	}

	for _, tt := range tests {

		testname := filepath.Base(tt.workflowFile)
		t.Run(testname, func(t *testing.T) {
			workflowByteValue, _ := pkg.ReadFile(tt.workflowFile)
			var workflow workflows.Workflow
			json.Unmarshal(workflowByteValue, &workflow)

			ans := workflows.TopologicalSort(&workflow)
			if ! reflect.DeepEqual(ans, tt.want) {
				t.Error("got " + strings.Join(ans[:], ",") + ", want " + strings.Join(tt.want[:], ","))
			}
		})
	}
}