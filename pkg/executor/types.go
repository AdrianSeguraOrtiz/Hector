package executor

type TaskStatus int32

const (
	Unknown TaskStatus = iota
	TaskSucceeded
	TaskSkipped
	TaskRunning
	TaskFailed
	TaskPending
	TaskDischarged
)

func (s *TaskStatus) String() string {
	switch *s {
	case TaskSucceeded:
		return "TaskSucceeded"
	case TaskSkipped:
		return "TaskSkipped"
	case TaskRunning:
		return "TaskRunning"
	case TaskFailed:
		return "TaskFailed"
	case TaskPending:
		return "TaskPending"
	case TaskDischarged:
		return "TaskDischarged"
	}
	return "Unknown"
}

type TaskResult struct {
	Status TaskStatus
}
