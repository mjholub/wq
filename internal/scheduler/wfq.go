package scheduler

import (
	"sort"
	"time"

	"github.com/mjholub/wq/internal/queue"
	"github.com/mjholub/wq/pkg/managedrefs"
)

// WFQSchedulable is a schedulable entity that can be scheduled by the WFQ scheduler.
type WFQSchedulable interface {
	// GetWeight returns the weight of the entity.
	// The weight is used to determine the share of CPU time the entity should receive.
	GetWeight() int

	// AddTask adds a task to the entity.
	AddTask(task *queue.Task)

	// GetTasks returns all the tasks in the entity.
	// The tasks are returned in the order they were added.
	GetTasks() []*queue.Task

	// Pop pops the next task from the entity.
	Pop() *queue.Task

	// LastFinish returns the time when the entity last finished executing a task.
	LastFinish() *managedrefs.ManagedRef[time.Time]

	// SetLastFinish sets the time when the entity last finished executing a task.
	SetLastFinish(time.Time)
}

type WFQScheduler struct {
	queues      []*queue.Queue
	virtualTime *managedrefs.ManagedRef[time.Time]
}

func NewWFQScheduler() *WFQScheduler {
	return &WFQScheduler{
		queues:      []*queue.Queue{},
		virtualTime: managedrefs.NewManagedRef(time.Now()),
	}
}

// AddQueue adds a queue to the scheduler.
// The queue is added to the end of the list of queues.
func (s *WFQScheduler) AddQueue(q *queue.Queue) {
	ql := len(s.queues)
	index := sort.Search(ql, func(i int) bool {
		return s.queues[i].Weight >= q.Weight
	})
	s.queues = append(s.queues[:index], append([]*queue.Queue{q}, s.queues[index:]...)...)
}

// RemoveQueue removes a queue from the scheduler.
func (s *WFQScheduler) RemoveQueue(q *queue.Queue) {
	ql := len(s.queues)
	index := sort.Search(ql, func(i int) bool {
		return s.queues[i].Weight >= q.Weight
	})

	if index < ql && s.queues[index] == q {
		s.queues = append(s.queues[:index], s.queues[index+1:]...)
	}
}

// GetQueueAt returns the queue at the given index.
func (s *WFQScheduler) GetQueueAt(index int) *queue.Queue {
	if index < 0 || index >= len(s.queues) {
		return nil
	}
	return s.queues[index]
}

// GetAllQueues returns all the queues in the scheduler.
func (s *WFQScheduler) GetAllQueues() []*queue.Queue {
	return s.queues
}

// GetNextTask returns the next task to be executed.
func (s *WFQScheduler) GetNextTask() *queue.Task {
	var nextTask *queue.Task
	virtualTime := s.virtualTime.Get()
	for _, q := range s.queues {
		if q.LastFinish().Get().Before(virtualTime) {
			q.SetLastFinish(virtualTime)
		}
		task := q.Pop()
		if task != nil {
			nextTask = task
			break
		}
	}
	return nextTask
}

// UpdateVirtualTime updates the virtual time of the scheduler.
// The virtual time is the time when the last task was executed.
func (s *WFQScheduler) UpdateVirtualTime(t time.Time) {
	s.virtualTime.Set(t)
}

// Schedule schedules the next task to be
// executed by the WFQ scheduler.
func (s *WFQScheduler) Schedule() *queue.Task {
	var selectedQueue *queue.Queue
	var task *queue.Task

	for _, q := range s.queues {
		q.Tasks.Update(func(tasks []*queue.Task) []*queue.Task {
			if len(tasks) > 0 && selectedQueue == nil {
				selectedQueue = q
				task = tasks[0]
				return tasks[1:]
			}
			return tasks
		})
	}

	if selectedQueue != nil {
		s.virtualTime.Update(func(vt time.Time) time.Time {
			return vt.Add(task.ExecTime * time.Duration(selectedQueue.Weight))
		})
		selectedQueue.LastFinish().Set(s.virtualTime.Get())
	}
	return task
}

// GetVirtualTime returns the virtual time of the scheduler.
func (s *WFQScheduler) GetVirtualTime() time.Time {
	return s.virtualTime.Get()
}
