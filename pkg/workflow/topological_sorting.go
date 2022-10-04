package workflow

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

func getChildren(id string, nodes *[]Task) []string {
	var children []string
	for _, node := range *nodes {
		if contains(node.Depends, id) {
			children = append(children, node.Name)
		}
	}
	return children
}

// TopologicalGroupedSort establishes a grouped topological order
// for an optimal and correct ordering of nodes defined on a DAG.
func TopologicalGroupedSort(wf *WorkflowSpec) ([][]string, error) {
	var zeroIndegree []string
	indegreeMap := make(map[string]int)
	// Fill both elements
	for _, node := range wf.Spec.Dag.Tasks {
		if node.Depends != nil {
			indegreeMap[node.Name] = len(node.Depends)
		} else {
			zeroIndegree = append(zeroIndegree, node.Name)
		}
	}
	var ordering [][]string
	// Add initial tasks (those without dependencies)
	ordering = append(ordering, zeroIndegree)
	finished := false
	for !finished {
		var newZeroIndegree []string
		for _, initialTaskName := range zeroIndegree {
			childrenNames := getChildren(initialTaskName, &wf.Spec.Dag.Tasks)
			for _, childName := range childrenNames {
				indegreeMap[childName] -= 1
				if indegreeMap[childName] == 0 {
					newZeroIndegree = append(newZeroIndegree, childName)
				}
			}
		}
		if len(newZeroIndegree) > 0 {
			ordering = append(ordering, newZeroIndegree)
			zeroIndegree = newZeroIndegree
		} else {
			finished = true
		}
	}
	return ordering, nil
}
