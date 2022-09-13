package main

import (
    "fmt"
    "encoding/json"
    "dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/workflows"
    "dag/hector/golang/module/pkg/executions"
    "dag/hector/golang/module/pkg/validators"
    "dag/hector/golang/module/pkg/executors"
    "dag/hector/golang/module/pkg/executors/execmock"
    "dag/hector/golang/module/pkg/databases/dbmock"
    "golang.org/x/exp/slices"
    "github.com/rs/xid"
    "flag"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
    // We obtain the path to the execution file provided as an input parameter
    var executionFile string
    flag.StringVar(&executionFile, "execution-file", "", "Execution json file path")
    flag.Parse()

    // We throw an error if not specified.
    if executionFile == "" {
        panic("Missing executionFile flag")
    }

    // We read the execution json file
	executionByteValue, err := pkg.ReadFile(executionFile)
	check(err)

    // Parse its content to the corresponding struct type
	var execution executions.Execution
	json.Unmarshal(executionByteValue, &execution)

    // We validate its structure
    validator := validators.Validator{}
    executionErr := validator.ValidateExecutionStruct(&execution)
    check(executionErr)

    // We print its contents for visual verification
	fmt.Printf("Execution: %+v\n\n", execution)

    // We extract the associated workflow and its topological order
    database := dbmock.DBMock{}
    execWorkflow, err := database.GetWorkflow(execution.Workflow)
    check(err)
    execTasksSorted, err := database.GetTopologicalSort(execution.Workflow)
    check(err)

    // We validate that the tasks required in the workflow are specified in the execution file
    taskValidatorErr := validator.ValidateExecutionTaskNames(&execution.Data.Tasks, &execWorkflow.Spec.Dag.Tasks)
    check(taskValidatorErr)
    
    // We build a two-dimensional vector to store the topologically ordered tasks with the necessary content for their execution.
    var runTasks [][]executors.RunTask

    // For each group of tasks ...
    for _, taskGroup := range execTasksSorted {

        // One-dimensional vector for storing group tasks
        var runTasksGroup []executors.RunTask

        // For each task within the group ...
        for _, taskName := range taskGroup {

            // A. We extract the task information from the execution file
            idxExecutionTask := slices.IndexFunc(execution.Data.Tasks, func(t executions.ExecutionTask) bool { return t.Name == taskName })
            executionTask := execution.Data.Tasks[idxExecutionTask]

            // B. We extract the task information from the workflow struct (mainly to know the identifier of its component)
            idxWorkflowTask := slices.IndexFunc(execWorkflow.Spec.Dag.Tasks, func(t workflows.WorkflowTask) bool { return t.Name == taskName })
            workflowTask := execWorkflow.Spec.Dag.Tasks[idxWorkflowTask]
            componentId := workflowTask.Component

            // C. We extract the information about the task component
            execComponent, err := database.GetComponent(componentId)
            check(err)

            // D. We check that the parameters entered (inputs/outputs) in the execution file are correct
            inputValidatorErr := validator.ValidateExecutionParameters(&executionTask.Inputs, &execComponent.Inputs)
            check(inputValidatorErr)
            outputValidatorErr := validator.ValidateExecutionParameters(&executionTask.Outputs, &execComponent.Outputs)
            check(outputValidatorErr)

            // E. We create the execution task
            task := executors.RunTask {
                Id: xid.New().String(),
                Name: taskName,
                Image: execComponent.Container.Image,
                Arguments: append(executionTask.Inputs, executionTask.Outputs ...),
            }

            // F. We add it to the group's task list
            runTasksGroup = append(runTasksGroup, task)
        }
        // We add the group's tasks to the two-dimensional list
        runTasks = append(runTasks, runTasksGroup)
    }

    // Instantiate the executor
    executor := execmock.ExecMock{}

    // For each group of tasks ...
    for _, runTaskGroup := range runTasks {
        // All group tasks are put into execution
        for _, runTask := range runTaskGroup {
            executor.ExecuteTask(&runTask)
        }
        // Wait for all group tasks to be completed before starting the next group
        for _, runTask := range runTaskGroup {
            executor.Wait(runTask.Id)
        }
    }
}