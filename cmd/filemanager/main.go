package main

import (
	"dag/hector/golang/module/pkg/filemanagers"
	"dag/hector/golang/module/pkg/filemanagers/minio"
	"flag"
	"fmt"
	"os"

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

	// Parameters
	var localPaths arrayFlags
	var remotePaths arrayFlags

	// First subcommand
	downloadCmd := flag.NewFlagSet("download", flag.ExitOnError)
	downloadCmd.Var(&localPaths, "local-path", "local paths")
	downloadCmd.Var(&remotePaths, "remote-path", "remote paths")

	// Second subcommand
	uploadCmd := flag.NewFlagSet("upload", flag.ExitOnError)
	uploadCmd.Var(&localPaths, "local-path", "local paths")
	uploadCmd.Var(&remotePaths, "remote-path", "remote paths")

	// Verify subcommand specification
	if len(os.Args) < 2 {
		fmt.Println("expected 'download' or 'upload' subcommands")
		os.Exit(1)
	}

	// Read environment variables
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}

	// Create FileManager
	var fileManager filemanagers.FileManager
	fileManager, err = minio.NewMinio()
	if err != nil {
		panic(err)
	}

	switch os.Args[1] {

	case "download":
		downloadCmd.Parse(os.Args[2:])
		if len(localPaths) != len(remotePaths) {
			panic("the number of remote and local routes must be the same")
		}
		for i := range localPaths {
			err := fileManager.DownloadFile(remotePaths[i], localPaths[i])
			if err != nil {
				panic(err)
			}
		}

	case "upload":
		uploadCmd.Parse(os.Args[2:])
		if len(localPaths) != len(remotePaths) {
			panic("the number of remote and local routes must be the same")
		}
		for i := range localPaths {
			err := fileManager.UploadFile(localPaths[i], remotePaths[i])
			if err != nil {
				panic(err)
			}
		}

	default:
		fmt.Println("expected 'download' or 'upload' subcommands")
		os.Exit(1)
	}

}
