package controller

import (
	"sync"

	"hector/pkg/executor"
	"hector/pkg/scheduler"
	"hector/pkg/workflow"
)

type Controller struct {
	strategy string

	executor *executor.Executor

	scheduler *scheduler.Scheduler

	recover bool
}

func NewController() *Controller {
	ex := executor.NewExecutor()
	sc := scheduler.NewScheduler()
	return &Controller{executor: ex, scheduler: sc, recover: false}
}

func (c *Controller) setState(uid string, state string) error {
	return nil
}

func (c *Controller) executePlan() error {
	return nil
}

func (c *Controller) checkUpstream() error {
	return nil
}

func (c *Controller) Invoke(wf *workflow.WorkflowSpec) (map[string]executor.TaskResult, error) {
	// Get the execution plan
	planning, err := c.scheduler.Plan(wf)
	if err != nil {
		return nil, err
	}
	mapConfig := map[string]workflow.Task{}
	for _, v := range wf.Spec.Dag.Tasks {
		mapConfig[v.Name] = v
	}
	states := make(map[string]executor.TaskResult, len(mapConfig))
	statesMux := &sync.RWMutex{}
	for _, taskGroup := range planning {
		var wg sync.WaitGroup
		for _, taskName := range taskGroup {
			task := mapConfig[taskName]

			// Skip if necessary.
			statesMux.RLock()
			state, ok := states[taskName]
			statesMux.RUnlock()
			if ok {
				if state.Status != executor.TaskPending {
					continue
				}
			}

			// Check dependants to determine if this task should be executed.
			shouldRun := true
			for _, dep := range task.Depends {
				statesMux.RLock()
				status := states[dep].Status
				statesMux.RUnlock()

				if status == executor.TaskDischarged || status == executor.TaskFailed || status == executor.TaskSkipped {
					shouldRun = false
					break
				}
			}

			if shouldRun {
				wg.Add(1)
				go func(t workflow.Task) {
					defer wg.Done()
					statesMux.Lock()
					states[t.Name], _ = c.executor.Run(&t)
					statesMux.Unlock()
				}(task)
			} else {
				statesMux.Lock()
				states[taskName] = executor.TaskResult{Status: executor.TaskSkipped}
				statesMux.Unlock()
			}
		}

		// Wait to collect results.
		wg.Wait()
	}
	return states, nil
}
