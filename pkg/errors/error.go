package errors

type ElementNotFoundErr struct {
	Type string
	Id   string
}

func (e *ElementNotFoundErr) Error() string {
	return e.Type + " with id " + e.Id + " not found in database."
}

type DuplicateIDErr struct {
	Type string
	Id   string
}

func (e *DuplicateIDErr) Error() string {
	return "A " + e.Type + " with id " + e.Id + " is already stored in the database."
}
