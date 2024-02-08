package errors

type ElementNotFoundErr struct {
	Type string
	Id   string
}

// Error function applied on a variable of type ElementNotFoundErr
// returns the corresponding error message in the form of string.
func (e *ElementNotFoundErr) Error() string {
	return e.Type + " with id " + e.Id + " not found in database."
}

type DuplicateIDErr struct {
	Type string
	Id   string
}

// Error function applied on a variable of type DuplicateIDErr
// returns the corresponding error message in the form of string.
func (e *DuplicateIDErr) Error() string {
	return "A " + e.Type + " with id " + e.Id + " is already stored in the database."
}
