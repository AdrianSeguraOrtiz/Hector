package pkg

import (
	"io/ioutil"
	"os"
)

func ReadFile(str string) ([]byte, error) {
	// Open jsonFile
	jsonFile, err := os.Open(str)

	// If os.Open returns an error then handle it
	if err != nil {
		return nil, err
	}

	// Defer the closing of the jsonFile
	defer jsonFile.Close()

	// Read the opened jsonFile as a byte array
	byteValue, _ := ioutil.ReadAll(jsonFile)

	// Return byteValue whitout errors
	return byteValue, nil
}

func Contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

func Ptr[T any](v T) *T { return &v }
