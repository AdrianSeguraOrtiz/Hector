package minio

import (
	"context"
	"fmt"
	"os"
	"strconv"

	minioSDK "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Minio struct {
	Client        *minioSDK.Client
	DefaultBucket string
}

// We create a specific constructor for our problem
func NewMinio() (*Minio, error) {
	min := Minio{}

	// Load environment variables
	endpoint := os.Getenv("MINIO_ENDPOINT")
	accessKeyID := os.Getenv("MINIO_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("MINIO_SECRET_ACCESS_KEY")
	useSSL, err := strconv.ParseBool(os.Getenv("MINIO_USE_SSL"))
	if err != nil {
		return nil, err
	}

	// Initialize minio client object.
	minioClient, err := minioSDK.New(endpoint, &minioSDK.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}

	// Insert client and default bucket to Minio struct instance
	min.Client = minioClient
	min.DefaultBucket = os.Getenv("MINIO_BUCKET_NAME")

	return &min, nil
}

// Download files from minio
func (min *Minio) DownloadFile(minioOrigFilePath string, localDestFilePath string) error {
	fmt.Println("Downloading " + minioOrigFilePath + " from Minio to " + localDestFilePath + " in local system")
	err := min.Client.FGetObject(context.Background(), min.DefaultBucket, minioOrigFilePath, localDestFilePath, minioSDK.GetObjectOptions{})
	return err
}

// Upload files to minio
func (min *Minio) UploadFile(localOrigFilePath string, minioDestFilePath string) error {
	fmt.Println("Uploading " + localOrigFilePath + " from local system to " + minioDestFilePath + " in Minio")
	_, err := min.Client.FPutObject(context.Background(), min.DefaultBucket, minioDestFilePath, localOrigFilePath, minioSDK.PutObjectOptions{})
	return err
}
