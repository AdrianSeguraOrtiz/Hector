package databases

import (
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/workflows"
	"dag/hector/golang/module/pkg/executions"
	"dag/hector/golang/module/pkg/executors"
	"dag/hector/golang/module/pkg/validators"
	"golang.org/x/exp/slices"
	"github.com/rs/xid"
)

type Database interface {
	GetWorkflow(id string) (workflows.Workflow, error)
	GetTopologicalSort(id string) ([][]string, error)
	GetComponent(id string) (components.Component, error)
    GetResultExecution(id string) (executors.ResultExecution, error)
    AddWorkflow(workflowPointer *workflows.Workflow) error
	AddExecution(executionPointer *executions.Execution) error
	AddResultExecution(resultExecutionPointer *executors.ResultExecution) error
}


func GetJobs(executionPointer *executions.Execution, databasePointer *Database, validatorPointer *validators.Validator) [][]executors.Job {
    // We extract the associated workflow and its topological order
    execWorkflow, err := (*databasePointer).GetWorkflow((*executionPointer).Workflow)
    pkg.Check(err)
    execTasksSorted, err := (*databasePointer).GetTopologicalSort((*executionPointer).Workflow)
    pkg.Check(err)

    // We validate that the tasks required in the workflow are specified in the execution file
    taskValidatorErr := (*validatorPointer).ValidateExecutionTaskNames(&(*executionPointer).Data.Tasks, &execWorkflow.Spec.Dag.Tasks)
    pkg.Check(taskValidatorErr)
    
    // We build a two-dimensional vector to store the topologically ordered tasks with the necessary content for their execution.
    var nestedJobs [][]executors.Job

    // For each group of tasks ...
    for _, taskGroup := range execTasksSorted {

        // One-dimensional vector for storing group tasks
        var jobsGroup []executors.Job

        // For each task within the group ...
        for _, taskName := range taskGroup {

            // A. We extract the task information from the execution file
            idxExecutionTask := slices.IndexFunc((*executionPointer).Data.Tasks, func(t executions.ExecutionTask) bool { return t.Name == taskName })
            executionTask := (*executionPointer).Data.Tasks[idxExecutionTask]

            // B. We extract the task information from the workflow struct (mainly to know the identifier of its component)
            idxWorkflowTask := slices.IndexFunc(execWorkflow.Spec.Dag.Tasks, func(t workflows.WorkflowTask) bool { return t.Name == taskName })
            workflowTask := execWorkflow.Spec.Dag.Tasks[idxWorkflowTask]
            componentId := workflowTask.Component

            // C. We extract the information about the task component
            execComponent, err := (*databasePointer).GetComponent(componentId)
            pkg.Check(err)

            // D. We check that the parameters entered (inputs/outputs) in the execution file are correct
            inputValidatorErr := (*validatorPointer).ValidateExecutionParameters(&executionTask.Inputs, &execComponent.Inputs)
            pkg.Check(inputValidatorErr)
            outputValidatorErr := (*validatorPointer).ValidateExecutionParameters(&executionTask.Outputs, &execComponent.Outputs)
            pkg.Check(outputValidatorErr)

            // E. We create the execution task (job)
            job := executors.Job {
                Id: xid.New().String(),
                Name: taskName,
                Image: execComponent.Container.Image,
                Arguments: append(executionTask.Inputs, executionTask.Outputs ...),
                Dependencies: workflowTask.Dependencies,
            }

            // F. We add it to the group's task list
            jobsGroup = append(jobsGroup, job)
        }
        // We add the group's tasks to the two-dimensional list
        nestedJobs = append(nestedJobs, jobsGroup)
    }

    return nestedJobs
}