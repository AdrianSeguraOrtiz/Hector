package workflows

import (
	"golang.org/x/exp/slices"
)

func topologicalSortUtil(name string, tasks *[]WorkflowTask, visited map[string]bool, stack *[]string) {
	/*
	This secondary function is responsible for exploring 
	dependency paths by means of recurrence.

	Receives the name of the starting task, the array pointer 
	with all tasks, the visit map (maps do not require pointers) 
	and the stack pointer.
	*/

	// The current task is marked as visited
	visited[name] = true

	// The data of the current task are obtained thanks to the name provided in the entry
	taskIndex := slices.IndexFunc(*tasks, func(t WorkflowTask) bool { return t.Name == name })
	task := (*tasks)[taskIndex]

	// Recurrence is applied for each dependency not reviewed.
	for _, dependencie := range task.Dependencies{
		if ! visited[dependencie] {
			topologicalSortUtil(dependencie, tasks, visited, stack)
		}
	}

	// The current task is added to the task stack.
	*stack = append(*stack, name)
}

func TopologicalSort(workflow *Workflow) []string {
	/*
	This function is responsible for extracting a feasible 
	topological order from the tasks in a workflow.

	Takes as input the pointer of a struct of type Workflow 
	and returns an ordered array of strings with the names of the tasks.
	*/

	// The task array is extracted from the value of the input pointer.
	tasks := (*workflow).Spec.Dag.Tasks

	// A map is created to indicate which tasks have already been reviewed (visited).
	visited := make(map[string]bool)

	// A stack is created to store the result
	var stack []string

	// For each unchecked task, the complementary function is called
    for _, task := range tasks {
        if ! visited[task.Name] {
			topologicalSortUtil(task.Name, &tasks, visited, &stack)
		}
    }

	// The completed stack is returned
	return stack
}