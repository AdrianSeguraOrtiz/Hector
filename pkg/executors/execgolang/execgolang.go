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

// readerToString function extracts the content of an io.ReadCloser variable and returns it as a string.
func readerToString(rc *io.ReadCloser) (string, error) {
	bytes, err := io.ReadAll(*rc)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// checkIfAvailable function checks if a certain image is available (built) in the system. To do so,
// it takes as input the classic Context variable, a pointer to the Client and the name of the image.
// Finally it returns a boolean value and an error variable in charge of notifying any problem.
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

// argumentsToSlice function takes Hector's own parameter definitions and converts
// them into an array of strings by adding dashes to the tags.
func argumentsToSlice(arguments *[]definitions.Parameter) []string {
	var args []string
	for _, arg := range *arguments {
		args = append(args, "--"+arg.Name)
		args = append(args, arg.Value.(string))
	}
	return args
}

type ExecGolang struct{}

// NewExecGolang function creates a new instance of the ExecGolang type. It
// returns a pointer to the constructed variable.
func NewExecGolang() *ExecGolang {
	return &ExecGolang{}
}

// ExecuteJob function executes a job locally. It takes as input the pointer
// of a given Job. It provides as output a pointer to the generated ResultJob
// and an error variable in charge of notifying any problem.
//
// Based on: https://docs.docker.com/engine/api/sdk/#sdk-and-api-quickstart and https://docs.docker.com/engine/api/sdk/examples/
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
		// TODO: Volumes: map[string]struct{}
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
