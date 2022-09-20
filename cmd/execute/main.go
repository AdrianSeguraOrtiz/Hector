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
    "sync"
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
        panic("Missing --execution-file flag")
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
    var nestedJobs [][]executors.Job

    // For each group of tasks ...
    for _, taskGroup := range execTasksSorted {

        // One-dimensional vector for storing group tasks
        var jobsGroup []executors.Job

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

    // Instantiate the executor
    executor := execmock.ExecMock{}

    // We create a map for storing the results of each job
    results := make(map[string]executors.Result)

    // For each group of tasks ...
    for _, jobGroup := range nestedJobs {

        // We create a waitgroup to allow waiting for all tasks belonging to the group
        var wg sync.WaitGroup

        // For each job in the group ...
        for _, job := range jobGroup {

            // If any of its dependencies has previously failed, the job is cancelled and its execution is dispensed with.
            cancelled := false
            for _, depName := range job.Dependencies {
                if results[depName].Status == executors.Error || results[depName].Status == executors.Cancelled{
                    cancelled = true
                    results[job.Name] = executors.Result{Id: job.Id, Logs: "Cancelled due to errors in its dependencies", Status: executors.Cancelled}
                    break
                }
            }

            // If none of its dependencies have previously failed, it is put into execution in a goroutine.
            /** 
                See https://gobyexample.com/waitgroups, 
                https://go.dev/doc/faq#closures_and_goroutines, 
                https://stackoverflow.com/questions/18499352/golang-concurrency-how-to-append-to-the-same-slice-from-different-goroutines
            */
            if !cancelled {
                wg.Add(1)
                go func(j executors.Job) {
                    results[j.Name] = executor.ExecuteJob(&j)
                    wg.Done()
                }(job)
            }
        }

        // Wait for all group tasks to be completed before starting the next group
        wg.Wait()
    }

    // Print results
    for k, v := range results {
        fmt.Printf(k + ": %+v\n", v)
    }
}