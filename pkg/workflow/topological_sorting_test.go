package workflow

import (
	"reflect"
	"testing"
)

func TestTopologicalGroupedSort(t *testing.T) {
	tasks := []Task{
		{Name: "A"},
		{Name: "B", Depends: []string{"A"}},
		{Name: "C", Depends: []string{"B"}},
		{Name: "D", Depends: []string{"C"}},
	}
	workflow := WorkflowSpec{
		Spec: Spec{
			Dag: Dag{
				Tasks: tasks,
			},
		},
	}

	ordering, _ := TopologicalGroupedSort(&workflow)
	want := [][]string{{"A"}, {"C"}, {"B"}, {"D"}}

	if !reflect.DeepEqual(ordering, want) {
		t.Error("Incorrect ordering")
	}
}
