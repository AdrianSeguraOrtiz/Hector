package topologicalgrouped

import (
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/specifications"
)

type TopologicalGrouped struct{}

func NewTopologicalGrouped() *TopologicalGrouped {
	return &TopologicalGrouped{}
}

func (tg *TopologicalGrouped) Plan(specification *specifications.Specification) ([][]string, error) {
	/*
		This function establishes a grouped topological order
		for an optimal and correct definition of tasks defined
		on a specification.

		Takes the specification pointer as input and returns a
		two-dimensional vector with the names of the sorted tasks.
	*/

	// The task array is extracted from the value of the input pointer.
	tasks := (*specification).Spec.Dag.Tasks

	// Declare a map containing for each task, the number of dependencies defined for it in the specification
	indegreeMap := make(map[string]int)
	// Declare an array with the names of tasks that do not have any dependency
	var zeroIndegree []string

	// Fill both elements
	for _, task := range tasks {
		if task.Dependencies != nil {
			indegreeMap[task.Name] = len(task.Dependencies)
		} else {
			zeroIndegree = append(zeroIndegree, task.Name)
		}
	}

	// Create the output vector
	var result [][]string
	// Add initial tasks (those without dependencies)
	result = append(result, zeroIndegree)
	// Create a Boolean variable that is activated when the search process is finished.
	var finished bool

	// Code based on response https://stackoverflow.com/a/56815903
	for !finished {
		var newZeroIndegree []string
		for _, initialTaskName := range zeroIndegree {
			childrenNames := getChildren(initialTaskName, &tasks)
			for _, childName := range childrenNames {
				indegreeMap[childName] -= 1
				if indegreeMap[childName] == 0 {
					newZeroIndegree = append(newZeroIndegree, childName)
				}
			}
		}
		if len(newZeroIndegree) > 0 {
			result = append(result, newZeroIndegree)
			zeroIndegree = newZeroIndegree
		} else {
			finished = true
		}
	}

	// Return the output vector
	return result, nil
}

func getChildren(taskName string, tasks *[]specifications.SpecificationTask) []string {
	/*
		This function is in charge of extracting the dependent tasks
		of the one specified in the entry.

		It takes as input the name of the task whose children you want
		to know, and the pointer to the total set of tasks in the specification.
		Finally it returns as result an array with the names of the
		children of the input task.
	*/

	// Declare the result list
	var children []string

	// Children are those tasks that contain the input task in their list of dependencies.
	for _, task := range *tasks {
		if pkg.Contains(task.Dependencies, taskName) {
			children = append(children, task.Name)
		}
	}

	// Return the result list
	return children
}
