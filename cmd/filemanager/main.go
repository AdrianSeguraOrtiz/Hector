package main

import (
	"dag/hector/golang/module/pkg/filemanagers"
	"dag/hector/golang/module/pkg/filemanagers/minio"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "my string representation"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {

	// Get working directory
	workingDir, wdErr := os.Getwd()
	if wdErr != nil {
		panic(wdErr)
	}

	// Parameters
	var envFile string
	var localPaths arrayFlags
	var remotePaths arrayFlags

	// First subcommand
	downloadCmd := flag.NewFlagSet("download", flag.ExitOnError)
	downloadCmd.StringVar(&envFile, "env", ".env", "env file path")
	downloadCmd.Var(&localPaths, "local-path", "local paths")
	downloadCmd.Var(&remotePaths, "remote-path", "remote paths")

	// Second subcommand
	uploadCmd := flag.NewFlagSet("upload", flag.ExitOnError)
	uploadCmd.StringVar(&envFile, "env", ".env", "env file path")
	uploadCmd.Var(&localPaths, "local-path", "local paths")
	uploadCmd.Var(&remotePaths, "remote-path", "remote paths")

	// Verify subcommand specification
	if len(os.Args) < 2 {
		fmt.Println("expected 'download' or 'upload' subcommands")
		os.Exit(1)
	}

	// Load variables
	switch os.Args[1] {
	case "download":
		downloadCmd.Parse(os.Args[2:])
	case "upload":
		uploadCmd.Parse(os.Args[2:])
	default:
		fmt.Println("expected 'download' or 'upload' subcommands")
		os.Exit(1)
	}

	// Verify the feasibility of route pairing
	if len(localPaths) != len(remotePaths) {
		panic("the number of remote and local routes must be the same")
	}

	// Read environment variables
	err := godotenv.Load(envFile)
	if err != nil {
		panic(err)
	}

	// Create FileManager
	var fileManager filemanagers.FileManager
	fileManager, err = minio.NewMinio()
	if err != nil {
		panic(err)
	}

	// Perform files download|upload
	switch os.Args[1] {
	case "download":
		for i := range localPaths {
			localFile, absErr := filepath.Abs(workingDir + "/" + localPaths[i])
			if absErr != nil {
				panic(err)
			}
			err := fileManager.DownloadFile(remotePaths[i], localFile)
			if err != nil {
				panic(err)
			}
		}
	case "upload":
		for i := range localPaths {
			localFile, absErr := filepath.Abs(workingDir + "/" + localPaths[i])
			if absErr != nil {
				panic(err)
			}
			err := fileManager.UploadFile(localFile, remotePaths[i])
			if err != nil {
				panic(err)
			}
		}
	default:
		fmt.Println("expected 'download' or 'upload' subcommands")
		os.Exit(1)
	}

}
