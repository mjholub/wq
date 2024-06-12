package queue

import (
	"time"

	"github.com/mjholub/wq/pkg/managedrefs"
	"github.com/oklog/ulid/v2"
)

type Task struct {
	ID        ulid.ULID
	Weight    uint64
	ExecTime  time.Duration
	Timestamp time.Time
}

type Queue struct {
	Tasks      *managedrefs.ManagedRef[[]*Task]
	Weight     int
	lastFinish *managedrefs.ManagedRef[time.Time]
}

func NewQueue(weight int) *Queue {
	return &Queue{
		Tasks:      managedrefs.NewManagedRef([]*Task{}),
		Weight:     weight,
		lastFinish: managedrefs.NewManagedRef(time.Now()),
	}
}

func (q *Queue) AddTask(task *Task) {
	q.Tasks.Update(func(tasks []*Task) []*Task {
		return append(tasks, task)
	})
}

func (q *Queue) GetTasks() []*Task {
	return q.Tasks.Get()
}

func (q *Queue) Pop() *Task {
	var task *Task
	q.Tasks.Update(func(tasks []*Task) []*Task {
		if len(tasks) == 0 {
			task = nil
			return tasks
		}
		task = tasks[0]
		return tasks[1:]
	})
	return task
}

func (q *Queue) LastFinish() *managedrefs.ManagedRef[time.Time] {
	return q.lastFinish
}

func (q *Queue) SetLastFinish(t time.Time) {
	q.lastFinish.Set(t)
}

func (q *Queue) GetWeight() int {
	return q.Weight
}
