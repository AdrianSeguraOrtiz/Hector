package filemanagers

import (
	"context"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Minio struct {
	Client        *minio.Client
	DefaultBucket string
}

// We create a specific constructor for our problem
func NewMinio() (*Minio, error) {
	min := Minio{}

	// Load environment variables
	endpoint := os.Getenv("MINIO_ENDPOINT")
	accessKeyID := os.Getenv("MINIO_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("MINIO_SECRET_ACCESS_KEY")
	useSSL := true

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
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
	err := min.Client.FGetObject(context.Background(), min.DefaultBucket, minioOrigFilePath, localDestFilePath, minio.GetObjectOptions{})
	return err
}

// Upload files to minio
func (min *Minio) UploadFile(localOrigFilePath string, minioDestFilePath string) error {
	_, err := min.Client.FPutObject(context.Background(), min.DefaultBucket, minioDestFilePath, localOrigFilePath, minio.PutObjectOptions{})
	return err
}
