package controller

import (
	"errors"
	"sync"

	"hector/pkg/executor"
)

type State struct {
	states map[string]executor.TaskResult

	statesMux *sync.RWMutex
}

func (s *State) Get(dep string) (*executor.TaskResult, error) {
	s.statesMux.RLock()
	state, ok := s.states[dep]
	s.statesMux.RUnlock()
	if !ok {
		return nil, errors.New("key not found")
	}
	return &state, nil
}

func (s *State) Set(dep string)
