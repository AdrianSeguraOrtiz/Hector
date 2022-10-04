package topologicalgrouped

import (
	"dag/hector/golang/module/pkg/specifications"
	"reflect"
	"strconv"
	"testing"
)

func TestTopologicalGroupedSort(t *testing.T) {
	var tests = []struct {
		tasks []specifications.SpecificationTask
		want  [][]string
	}{
		{[]specifications.SpecificationTask{
			{Name: "A"},
			{Name: "B", Dependencies: []string{"C"}},
			{Name: "C", Dependencies: []string{"A"}},
			{Name: "D", Dependencies: []string{"B"}},
		}, [][]string{{"A"}, {"C"}, {"B"}, {"D"}}},

		// Based on https://www.geeksforgeeks.org/topological-sorting/ DAG
		{[]specifications.SpecificationTask{
			{Name: "0", Dependencies: []string{"5", "4"}},
			{Name: "1", Dependencies: []string{"4", "3"}},
			{Name: "2", Dependencies: []string{"5"}},
			{Name: "3", Dependencies: []string{"2"}},
			{Name: "4"},
			{Name: "5"},
		}, [][]string{{"4", "5"}, {"0", "2"}, {"3"}, {"1"}}},

		// Based on https://upload.wikimedia.org/wikipedia/commons/0/08/Directed_acyclic_graph.png
		{[]specifications.SpecificationTask{
			{Name: "2", Dependencies: []string{"11"}},
			{Name: "3"},
			{Name: "5"},
			{Name: "7"},
			{Name: "8", Dependencies: []string{"7", "3"}},
			{Name: "9", Dependencies: []string{"11", "8"}},
			{Name: "10", Dependencies: []string{"11", "3"}},
			{Name: "11", Dependencies: []string{"7", "5"}},
		}, [][]string{{"3", "5", "7"}, {"8", "11"}, {"2", "9", "10"}}},
	}

	for i, tt := range tests {

		testname := "test_" + strconv.Itoa(i)
		t.Run(testname, func(t *testing.T) {
			specification := specifications.Specification{
				Spec: specifications.Spec{
					Dag: specifications.Dag{
						Tasks: tt.tasks,
					},
				},
			}

			tg := NewTopologicalGrouped()
			ans, _ := tg.Plan(&specification)
			if !reflect.DeepEqual(ans, tt.want) {
				t.Error("got ", ans, ", want ", tt.want[:])
			}
		})
	}
}
