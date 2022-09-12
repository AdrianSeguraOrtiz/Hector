package main

import (
    "fmt"
    "encoding/json"
    "reflect"
    "dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/workflows"
    "dag/hector/golang/module/pkg/executions"
    "github.com/go-playground/validator/v10"
    "golang.org/x/exp/slices"
)

func main() {
    // Leemos y validamos todos los componentes y el workflow para almacenarlos en variables (simulando el contenido de una futura base de datos)
    v := validator.New()
	v.RegisterValidation("representsType", components.RepresentsType)
	v.RegisterValidation("validDependencies", workflows.ValidDependencies)

    componentFiles := []string{"./data/hector/toy_components/concat_files/concat-files-component.json", "./data/hector/toy_components/concat_messages/concat-messages-component.json", "./data/hector/toy_components/count_letters/count-letters-component.json"}
    componentStructs := make([]components.Component, 0)

    for _, f := range componentFiles {
        componentByteValue, err := pkg.ReadFile(f)
        if err != nil {
            fmt.Println(err)
        }

        var component components.Component
        json.Unmarshal(componentByteValue, &component)

        componentErr := v.Struct(component)
        if componentErr != nil {
            fmt.Println(componentErr)
        }

        componentStructs = append(componentStructs, component)
	}

    workflowFiles := []string{"./data/hector/toy_workflows/toy_workflow_1.json"}
    workflowStructs := make([]workflows.Workflow, 0)

    for _, f := range workflowFiles {
        workflowByteValue, err := pkg.ReadFile(f)
        if err != nil {
            fmt.Println(err)
        }

        var workflow workflows.Workflow
        json.Unmarshal(workflowByteValue, &workflow)

        workflowErr := v.Struct(workflow)
        if workflowErr != nil {
            fmt.Println(workflowErr)
        }

        workflowStructs = append(workflowStructs, workflow)
	}

    // Extraemos el orden topológico de los workflows y simulamos el contenido de una segunda base de datos
    topologicalSortOfWorkflows := make(map[string][][]string)
    for _, w := range workflowStructs {
        topologicalSortOfWorkflows[w.Id] = workflows.TopologicalGroupedSort(&w)
    }

    // Aquí empezaría el verdadero código
    // 1. Leemos el json de ejecución
    executionFile := "./data/hector/toy_executions/toy_execution_1.json"
	executionByteValue, err := pkg.ReadFile(executionFile)
	if err != nil {
		fmt.Println(err)
	}

	var execution executions.Execution
	json.Unmarshal(executionByteValue, &execution)

	fmt.Printf("Execution: %+v\n\n", execution)

    // 2. Extraemos el workflow que ejecuta y su orden topológico
    idx := slices.IndexFunc(workflowStructs, func(w workflows.Workflow) bool { return w.Id == execution.Workflow })
    execWorkflow := workflowStructs[idx]
    execTasksSorted := topologicalSortOfWorkflows[execution.Workflow]

    // 3. Recorremos las agrupaciones de tareas según el orden topológico del workflow
    for _, taskGroup := range execTasksSorted {
        // Para cada tarea dentro del grupo ...
        for _, taskName := range taskGroup {
            // A. Extraemos la información de la tarea del archivo de ejecución (si no se encuentra se lanza un error)
            idxExecutionTask := slices.IndexFunc(execution.Data.Tasks, func(t executions.ExecutionTask) bool { return t.Name == taskName })
            if idxExecutionTask == -1 {
                panic("Task " + taskName + " is required in the selected workflow but is not present in the execution file.")
            }
            executionTask := execution.Data.Tasks[idxExecutionTask]

            // B. Extraemos la información de la tarea reflejada en el workflow (principalmente para conocer el identificador de su componente)
            idxWorkflowTask := slices.IndexFunc(execWorkflow.Spec.Dag.Tasks, func(t workflows.WorkflowTask) bool { return t.Name == taskName })
            workflowTask := execWorkflow.Spec.Dag.Tasks[idxWorkflowTask]
            componentId := workflowTask.Component

            // C. Extraemos la información acerca del componente de la tarea y comprobamos que los parámetros introducidos (inputs/outputs) en el archivo de ejecución son correctos
            idxExecComponent := slices.IndexFunc(componentStructs, func(c components.Component) bool { return c.Id == componentId })
            execComponent := componentStructs[idxExecComponent]

            for _, componentInput := range execComponent.Inputs {
                idxExecutionInput := slices.IndexFunc(executionTask.Inputs, func(p executions.Parameter) bool { return p.Name == componentInput.Name })
                if idxExecutionInput == -1 {
                    panic("Input " + componentInput.Name + " is required in the " + taskName + " task but is not present in the execution file.")
                }
                executionInput := executionTask.Inputs[idxExecutionInput]
                if reflect.TypeOf(executionInput.Value).String() != componentInput.Type {
                    panic("Input " + componentInput.Name + " has an invalid value in the execution file.")
                }
            }

            for _, componentOutput := range execComponent.Outputs {
                idxExecutionOutput := slices.IndexFunc(executionTask.Outputs, func(p executions.Parameter) bool { return p.Name == componentOutput.Name })
                if idxExecutionOutput == -1 {
                    panic("Output " + componentOutput.Name + " is required in the " + taskName + " task but is not present in the execution file.")
                }
                executionOutput := executionTask.Outputs[idxExecutionOutput]
                if reflect.TypeOf(executionOutput.Value).String() != componentOutput.Type {
                    panic("Output " + componentOutput.Name + " has an invalid value in the execution file.")
                }
            }
        }
        
        // Simular la ejecución del grupo de tareas
    }
}