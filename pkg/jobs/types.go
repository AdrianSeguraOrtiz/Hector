package jobs

import "dag/hector/golang/module/pkg/definitions"

type Job struct {
	Id           string
	Name         string
	Image        string
	Arguments    []definitions.Parameter
	Dependencies []string
}
