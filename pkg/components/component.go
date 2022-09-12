package components

type Put struct {
	Name string `json:"name" validate:"required"`
	Type string `json:"type" validate:"required,representsType"`
}

type Container struct {
	Dockerfile 	string		`json:"dockerfile" validate:"required"`
	Image 		string		`json:"image" validate:"required"`
	Command	 	[]string	`json:"command"`
}

type Component struct {
    Id				string		`json:"id" validate:"required"`
    Name			string		`json:"name" validate:"required"`
    ApiVersion		string		`json:"apiVersion" validate:"required"`
	Inputs			[]Put		`json:"inputs" validate:"dive"`
	Outputs			[]Put		`json:"outputs" validate:"dive"`
	Container	 	Container	`json:"container" validate:"required"`
}