package execgolang

import (
	"context"
	"io"
	"fmt"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"dag/hector/golang/module/pkg"
	"dag/hector/golang/module/pkg/executors"
	"dag/hector/golang/module/pkg/executions"
)

func readerToString(rcPointer *io.ReadCloser) (string, error) {	
	bytes, err := io.ReadAll(*rcPointer)
	if (err != nil) {
		return "", err
	}
	return string(bytes), nil
}

func checkIfAvailable(ctx context.Context, cliPointer *client.Client, image string) (bool, error) {
	images, err := (*cliPointer).ImageList(ctx, types.ImageListOptions{})
	if (err != nil) {
		return false, err
	}

	for _, img := range images {
		for _, name := range img.RepoTags {
			if image == name {
				return true, nil
			}
		}
	}

	return false, nil
}

func argumentsToSlice(argumentsPointer *[]executions.Parameter) []string {
	var args []string
	for _, arg := range *argumentsPointer {
		args = append(args, "--" + arg.Name)
		args = append(args, arg.Value.(string))
	}
	return args
}

type ExecGolang struct {}

func (eg *ExecGolang) ExecuteJob(jobPointer *executors.Job) executors.ResultJob {
	/*
		This function executes a job locally.
		Based on: https://docs.docker.com/engine/api/sdk/#sdk-and-api-quickstart and https://docs.docker.com/engine/api/sdk/examples/
	*/

	// We print the initialization message and display the job information
	fmt.Printf("Started " + (*jobPointer).Name + " job. Info: \n\t %+v\n\n", *jobPointer)

	// We create the variable logs to store all the information associated with the execution of the job
	var logs string

	// Start context
	ctx := context.Background()

	// Start docker client
    cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
    pkg.Check(err)

	// Pull image in case it is not available in the system
	available, err := checkIfAvailable(ctx, cli, (*jobPointer).Image)
	pkg.Check(err)
	if !available {
		reader, err := cli.ImagePull(ctx, (*jobPointer).Image, types.ImagePullOptions{})
		pkg.Check(err)
		pullLogs, err := readerToString(&reader)
		pkg.Check(err)
		logs += pullLogs + "\n"
	}

	// We create the container by specifying the image and the job arguments
	args := argumentsToSlice(&(*jobPointer).Arguments)
    resp, err := cli.ContainerCreate(ctx, &container.Config{
        Image: (*jobPointer).Image,
		Cmd: args,
    }, nil, nil, nil, "")
    pkg.Check(err)

	// We run the container
	errCS := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{})
    pkg.Check(errCS)

	// We wait for its execution to be completed.
    statusCh, errCh := cli.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
    select {
		case err := <-errCh:
			pkg.Check(err)
		case <-statusCh:
    }

	// We print the finalization message
	fmt.Println("Finished " + (*jobPointer).Name + " job\n")

	// If the execution has reported contents in the error stream, the execution is considered failed.
    errorReader, err := cli.ContainerLogs(ctx, resp.ID, types.ContainerLogsOptions{ShowStderr: true})
    pkg.Check(err)
	errorLogs, err := readerToString(&errorReader)
	pkg.Check(err)
	if errorLogs != "" {
		logs += errorLogs
		return executors.ResultJob{Id: (*jobPointer).Id, Name: (*jobPointer).Name, Logs: logs, Status: executors.Error}
	}

	// Otherwise, the contents of the output stream are retrieved and the execution is considered successful.
	execReader, err := cli.ContainerLogs(ctx, resp.ID, types.ContainerLogsOptions{ShowStdout: true})
    pkg.Check(err)
	execLogs, err := readerToString(&execReader)
	pkg.Check(err)
	logs += execLogs
	return executors.ResultJob{Id: (*jobPointer).Id, Name: (*jobPointer).Name, Logs: logs, Status: executors.Done}
}