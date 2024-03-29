package pkg

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
)

// ReadFile function reads a file and returns the array of bytes
// corresponding to its contents. Takes the file path as input. Returns
// the array of bytes and an error variable to report any problems.
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

// Contains function asserts or denies the presence of a given string
// in an array of strings. Takes as input the string and the string array.
// Returns a boolean variable.
func Contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

// Ptr function stores the input value in a variable and returns the pointer
// to it. It takes any value as input. Returns a pointer.
func Ptr[T any](v T) *T { return &v }

// Compare function affirms or disaffirms the equality of two variables. It contains
// an additional boolean variable applied in case of receiving input pointers that
// allows to specify whether to measure their equality by their values or respective
// references. If the variables in question are composite (made up of simpler variables)
// the DeepValueEqual function is called to compare each of their constituent parts.
// It takes as input two variables of any class for comparison and the additional boolean
// variable relating to the pointers. Returns as output a boolean variable and a string
// variable that records any distinction between the variables, providing a justification
// of the answer.
func Compare(a, b any, compareValuesInsteadOfPointers bool) (bool, string) {
	var comparison bool
	var message string

	if kind := reflect.ValueOf(a).Kind(); kind == reflect.Slice || kind == reflect.Struct || kind == reflect.Map {
		comparison, message = DeepValueEqual(a, b, compareValuesInsteadOfPointers)
	} else {
		comparison = a == b
	}

	if !comparison {
		message = "Values " + fmt.Sprintf("%v", a) + ", " + fmt.Sprintf("%v", b) + " do not match. \n" + message
	}

	return comparison, message
}

// DeepValueEqual function affirms or disaffirms the equality between two composite variables.
// To do so, it recursively calls the Compare function by instantiating it with the simple
// variables that constitute them. It contains an additional boolean variable relative to the
// pointers that allows to specify whether their equality will depend on their values or
// references. It takes as input two variables of any of the following types: Slice, Map or
// Struct, and the boolean variable relative to the pointers. For output it returns a boolean
// variable and a string variable that records any distinction between the variables, providing
// a justification of the answer.
func DeepValueEqual(nested1, nested2 any, compareValuesInsteadOfPointers bool) (bool, string) {
	// Get reflect value of each input parameter
	v1 := reflect.ValueOf(nested1)
	v2 := reflect.ValueOf(nested2)

	// Declare the lists of keys to be used in case the input variables are maps
	keys1 := []string{}
	keys2 := []string{}

	// We extract the length of the variables according to their type, finally checking that it is one of the allowed ones (struct, map or slice)
	var l1, l2 int
	if v1.Kind() == reflect.Struct && v2.Kind() == reflect.Struct {
		l1 = v1.NumField()
		l2 = v2.NumField()
	} else if (v1.Kind() == reflect.Slice && v2.Kind() == reflect.Slice) || (v1.Kind() == reflect.Map && v2.Kind() == reflect.Map) {
		l1 = v1.Len()
		l2 = v2.Len()
	} else {
		return false, "Invalid input types"
	}

	// If their lengths are different, they will definitely have different content
	if l1 != l2 {
		return false, "The number of fields does not match"
	}

	// In case they are maps (we have previously checked that both are the same) ...
	if v1.Kind() == reflect.Map {
		// We extract the keys
		for _, v := range v1.MapKeys() {
			keys1 = append(keys1, v.String())
		}
		for _, v := range v2.MapKeys() {
			keys2 = append(keys2, v.String())
		}

		// Sort them alphabetically
		sort.Strings(keys1)
		sort.Strings(keys2)

		// Check that they are equal
		if !reflect.DeepEqual(keys1, keys2) {
			return false, "Keys of " + fmt.Sprintf("%v", v1) + " and " + fmt.Sprintf("%v", v2) + " do not match"
		}
	}

	// For each object of both variables ...
	for i := 0; i < l1; i++ {

		// We extract the reflect value of both objects depending on the type of the input variables
		var fieldS1, fieldS2 reflect.Value
		if v1.Kind() == reflect.Slice {
			fieldS1 = v1.Index(i)
			fieldS2 = v2.Index(i)
		} else if v1.Kind() == reflect.Struct {
			fieldS1 = v1.Field(i)
			fieldS2 = v2.Field(i)
		} else {
			fieldS1 = v1.MapIndex(reflect.ValueOf(keys1[i]))
			fieldS2 = v2.MapIndex(reflect.ValueOf(keys2[i]))
		}

		// If the type of both objects is not the same, they are not equal.
		if fieldS1.Kind() != fieldS2.Kind() {
			return false, "Variable type does not match"
		}

		// If they are pointers and compareValuesInsteadOfPointers is activated, we extract their values
		if compareValuesInsteadOfPointers && fieldS1.Kind() == reflect.Ptr {
			fieldS1 = fieldS1.Elem()
			fieldS2 = fieldS2.Elem()
		}

		// If the values are non-zero, we compare them by means of the complementary function
		if fieldS1.IsValid() && fieldS2.IsValid() {
			comparison, message := Compare(fieldS1.Interface(), fieldS2.Interface(), compareValuesInsteadOfPointers)
			if !comparison {
				return false, message
			}
			// In case one of the two is null and the other is not, they are not equal
		} else if (!fieldS1.IsValid() && fieldS2.IsValid()) || (fieldS1.IsValid() && !fieldS2.IsValid()) {
			return false, "Values do not match. "
		}
	}

	// If the execution has not been interrupted at any time, both input variables have the same content
	return true, ""
}
