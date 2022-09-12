package executions

type Parameter struct {
	Name string `json:"name" validate:"required"` // Queda pendiente si añadir una función que valide si el nombre del input/output se encuentra en el componente asociado a la tarea señalada
	Value interface{} `json:"value" validate:"required"` // Queda pendiente si añadir una función que valide si el tipo del valor se corresponde con el definido en el componente asociado a la tarea señalada
}

type ExecutionTask struct {
	Name 		string 		`json:"name" validate:"required"` // Queda pendiente si añadir una función que valide si el nombre de la tarea se encuentra en el workflow que pretende ejecutar
	Inputs 		[]Parameter `json:"inputs" validate:"dive"` 
	Outputs 	[]Parameter `json:"outputs" validate:"dive"`
}

type Data struct {
	Tasks []ExecutionTask `json:"tasks" validate:"dive"`
}

type Execution struct {
    Id				string		`json:"id" validate:"required"`
    Name			string		`json:"name" validate:"required"`
	Workflow		string		`json:"workflow" validate:"required"` // Queda pendiente si añadir una función que valide la existencia del workflow
    ApiVersion		string		`json:"apiVersion" validate:"required"`
	Data			Data		`json:"data" validate:"dive"`
}