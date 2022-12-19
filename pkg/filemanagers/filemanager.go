package filemanagers

type FileManager interface {
	DownloadFile(minioOrigFilePath string, localDestFilePath string) error
	UploadFile(localOrigFilePath string, minioDestFilePath string) error
}
