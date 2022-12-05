package execgolang

import (
	"context"
	"fmt"
	"io"

	"dag/hector/golang/module/pkg/definitions"
	"dag/hector/golang/module/pkg/jobs"
	"dag/hector/golang/module/pkg/results"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

func readerToString(rc *io.ReadCloser) (string, error) {
	bytes, err := io.ReadAll(*rc)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func checkIfAvailable(ctx context.Context, cli *client.Client, image string) (bool, error) {
	images, err := cli.ImageList(ctx, types.ImageListOptions{})
	if err != nil {
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

func argumentsToSlice(arguments *[]definitions.Parameter) []string {
	var args []string
	for _, arg := range *arguments {
		args = append(args, "--"+arg.Name)
		args = append(args, arg.Value.(string))
	}
	return args
}

type ExecGolang struct{}

func NewExecGolang() *ExecGolang {
	return &ExecGolang{}
}

/**
This function executes a job locally.
Based on: https://docs.docker.com/engine/api/sdk/#sdk-and-api-quickstart and https://docs.docker.com/engine/api/sdk/examples/
*/
func (eg *ExecGolang) ExecuteJob(job *jobs.Job) (*results.ResultJob, error) {

	// We print the initialization message and display the job information
	fmt.Printf("Started "+job.Name+" job. Info: \n\t %+v\n\n", *job)

	// We create the variable logs to store all the information associated with the definition of the job
	var logs string

	// Start context
	ctx := context.Background()

	// Start docker client
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}

	// Pull image in case it is not available in the system
	available, err := checkIfAvailable(ctx, cli, job.Image)
	if err != nil {
		return nil, err
	}
	if !available {
		reader, err := cli.ImagePull(ctx, job.Image, types.ImagePullOptions{})
		if err != nil {
			return &results.ResultJob{Id: job.Id, Name: job.Name, Logs: err.Error(), Status: results.Error}, nil
		}
		pullLogs, err := readerToString(&reader)
		if err != nil {
			return nil, err
		}
		logs += pullLogs + "\n"
	}

	// We create the container by specifying the image and the job arguments
	args := argumentsToSlice(&job.Arguments)
	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image: job.Image,
		Cmd:   args,
	}, nil, nil, nil, "")
	if err != nil {
		return nil, err
	}

	// We run the container
	ContStartErr := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{})
	if ContStartErr != nil {
		return nil, ContStartErr
	}

	// We wait for its definition to be completed.
	statusCh, errCh := cli.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return nil, err
		}
	case <-statusCh:
	}

	// We print the finalization message
	fmt.Println("Finished " + job.Name + " job\n")

	// If the definition has reported contents in the error stream, the definition is considered failed.
	errorReader, err := cli.ContainerLogs(ctx, resp.ID, types.ContainerLogsOptions{ShowStderr: true})
	if err != nil {
		return nil, err
	}
	errorLogs, err := readerToString(&errorReader)
	if err != nil {
		return nil, err
	}
	if errorLogs != "" {
		logs += errorLogs
		return &results.ResultJob{Id: job.Id, Name: job.Name, Logs: logs, Status: results.Error}, nil
	}

	// Otherwise, the contents of the output stream are retrieved and the definition is considered successful.
	execReader, err := cli.ContainerLogs(ctx, resp.ID, types.ContainerLogsOptions{ShowStdout: true})
	if err != nil {
		return nil, err
	}
	execLogs, err := readerToString(&execReader)
	if err != nil {
		return nil, err
	}
	logs += execLogs
	return &results.ResultJob{Id: job.Id, Name: job.Name, Logs: logs, Status: results.Done}, nil
}
