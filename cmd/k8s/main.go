package main

import (
    "context"
    "log"
    "os"
    "path/filepath"
    "strings"
    "fmt"
    "encoding/json"
    "dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/components"
	"dag/hector/golang/module/pkg/workflows"
    "dag/hector/golang/module/pkg/executions"
    "github.com/go-playground/validator/v10"
    "golang.org/x/exp/slices"

    batchv1 "k8s.io/api/batch/v1"
    v1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    kubernetes "k8s.io/client-go/kubernetes"
    clientcmd "k8s.io/client-go/tools/clientcmd"
)

func connectToK8s() *kubernetes.Clientset {
    home, exists := os.LookupEnv("HOME")
    if !exists {
        home = "/root"
    }

    configPath := filepath.Join(home, ".kube", "config")

    config, err := clientcmd.BuildConfigFromFlags("", configPath)
    if err != nil {
        log.Fatalln("failed to create K8s config")
    }

    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        log.Fatalln("Failed to create K8s clientset")
    }

    return clientset
}

func launchK8sJob(clientset *kubernetes.Clientset, jobName *string, image *string, args *string) {
    jobs := clientset.BatchV1().Jobs("default")
	var completions int32 = 1
	var parallelism int32 = 1

    jobSpec := &batchv1.Job{
        ObjectMeta: metav1.ObjectMeta{
            Name:      *jobName,
        },
        Spec: batchv1.JobSpec{
			Completions: &completions,
  			Parallelism: &parallelism,
            Template: v1.PodTemplateSpec{
                Spec: v1.PodSpec{
                    Containers: []v1.Container{
                        {
                            Name:    *jobName,
                            Image:   *image,
                            Args:    strings.Split(*args, " "),
                        },
                    },
                    RestartPolicy: v1.RestartPolicyNever,
                },
            },
        },
    }

    _, err := jobs.Create(context.TODO(), jobSpec, metav1.CreateOptions{})
    if err != nil {
        log.Fatalln("Failed to create K8s job. " + err.Error())
    }

    log.Println("Created K8s job successfully")
}

func main() {
    // Leemos y validamos todos los componentes y el workflow para almacenarlos en variables (simulando el contenido de una futura base de datos)
    v := validator.New()
	v.RegisterValidation("representsType", components.RepresentsType)
	v.RegisterValidation("validDependencies", workflows.ValidDependencies)

    componentFiles := []string{"./data/kubernetes/toy_components/concat_files/concat-files-component.json", "./data/kubernetes/toy_components/concat_messages/concat-messages-component.json", "./data/kubernetes/toy_components/count_letters/count-letters-component.json"}
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

    workflowFiles := []string{"./data/kubernetes/toy_workflows/toy_workflow_1.json"}
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
    topologicalSortOfWorkflows := make(map[string][]string)
    for _, w := range workflowStructs {
        topologicalSortOfWorkflows[w.Id] = workflows.TopologicalSort(&w)
    }

    // Aquí empezaría el verdadero código
    // 1. Leemos el json de ejecución
    executionFile := "./data/kubernetes/toy_executions/toy_execution_1.json"
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

    // 3. Recorremos en orden cada una de las tareas según su orden topológico
    for _, taskName := range execTasksSorted {
        idxExecutionTask := slices.IndexFunc(execution.Data.Tasks, func(t ExecutionTask) bool { return t.Name == taskName })
        if idxExecutionTask == -1 {
            Panic
        }
        executionTask :=
        workflowTask :=
        execComponent :=
    }

    // 3.1. Comprobamos que la tarea se ha especificado correctamente en el json de ejecución

    // 3.2. Convertimos la tarea en un specjob de kubernetes

    // 4. Ponemos a ejecutar en orden los specjobs
    //clientset := connectToK8s()
    //launchK8sJob(clientset, jobName, containerImage, arguments)
}