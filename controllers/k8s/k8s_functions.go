package k8s

// https://dev.to/narasimha1997/create-kubernetes-jobs-in-golang-using-k8s-client-go-api-59ej

import (
	"context"
    "log"
    "os"
    "path/filepath"
    "strings"

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