package specifications

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestTopologicalGroupedSort(t *testing.T) {
	var tests = []struct {
		specificationFile string
		want              [][]string
	}{
		{"./../../data/hector/test_topological_sort/test_topological_sort_1.json", [][]string{{"A"}, {"C"}, {"B"}, {"D"}}},
		{"./../../data/hector/test_topological_sort/test_topological_sort_2.json", [][]string{{"4", "5"}, {"0", "2"}, {"3"}, {"1"}}},           // Based on https://www.geeksforgeeks.org/topological-sorting/ DAG
		{"./../../data/hector/test_topological_sort/test_topological_sort_3.json", [][]string{{"3", "5", "7"}, {"8", "11"}, {"2", "9", "10"}}}, // Based on https://upload.wikimedia.org/wikipedia/commons/0/08/Directed_acyclic_graph.png
	}

	for _, tt := range tests {

		testname := filepath.Base(tt.specificationFile)
		t.Run(testname, func(t *testing.T) {
			var specification Specification
			specification.FromFile(tt.specificationFile)

			ans := TopologicalGroupedSort(&specification)
			if !reflect.DeepEqual(ans, tt.want) {
				t.Error("got ", ans, ", want ", tt.want[:])
			}
		})
	}
}
