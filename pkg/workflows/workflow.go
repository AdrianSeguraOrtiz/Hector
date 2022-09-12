package workflows

type WorkflowTask struct {
	Name 			string 		`json:"name" validate:"required"`
	Dependencies 	[]string 	`json:"dependencies"`
	Component 		string 		`json:"component" validate:"required"` // Queda pendiente si añadir una función que valide la existencia del componente
}

type Dag struct {
	Tasks []WorkflowTask `json:"tasks" validate:"required,min=1,validDependencies,dive"`
}

type Spec struct {
	Dag Dag `json:"dag" validate:"required"`
}

type Workflow struct {
    Id				string		`json:"id" validate:"required"`
    Name			string		`json:"name" validate:"required"`
    ApiVersion		string		`json:"apiVersion" validate:"required"`
	Spec			Spec		`json:"spec" validate:"required"`
}