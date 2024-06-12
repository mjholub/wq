package scheduler_test

import (
	"testing"
	"time"

	"github.com/mjholub/wq/internal/queue"
	"github.com/mjholub/wq/internal/scheduler"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWFQScheduler(t *testing.T) {
	s := scheduler.NewWFQScheduler()
	require.NotNil(t, s)
	assert.NotNil(t, s.GetVirtualTime())
	assert.Empty(t, s.GetAllQueues())
}

func TestAddQueue(t *testing.T) {
	s := scheduler.NewWFQScheduler()

	q0 := queue.NewQueue(10)
	q1 := queue.NewQueue(20)

	s.AddQueue(q0)
	s.AddQueue(q1)

	queues := s.GetAllQueues()
	require.Len(t, queues, 2)

	assert.Equal(t, q0, queues[0])
	assert.Equal(t, q1, s.GetAllQueues()[1])
}

func TestRemoveQueue(t *testing.T) {
	s := scheduler.NewWFQScheduler()

	q0 := queue.NewQueue(10)
	q1 := queue.NewQueue(20)

	s.AddQueue(q0)
	s.AddQueue(q1)

	s.RemoveQueue(q0)

	queues := s.GetAllQueues()
	require.Len(t, queues, 1)

	assert.Equal(t, q1, queues[0])
}

func TestGetQueueAt(t *testing.T) {
	scheduler := scheduler.NewWFQScheduler()

	q1 := queue.NewQueue(10)
	scheduler.AddQueue(q1)

	assert.Equal(t, q1, scheduler.GetQueueAt(0))
	assert.Nil(t, scheduler.GetQueueAt(1))
	assert.Nil(t, scheduler.GetQueueAt(-1))
}

func TestGetNextTask(t *testing.T) {
	scheduler := scheduler.NewWFQScheduler()

	q1 := queue.NewQueue(10)
	q2 := queue.NewQueue(20)
	scheduler.AddQueue(q1)
	scheduler.AddQueue(q2)

	task1 := &queue.Task{ID: ulid.Make(), Weight: 1, ExecTime: 1 * time.Second}
	task2 := &queue.Task{ID: ulid.Make(), Weight: 1, ExecTime: 1 * time.Second}
	q1.AddTask(task1)
	q2.AddTask(task2)

	scheduler.UpdateVirtualTime(time.Now())
	task := scheduler.GetNextTask()

	assert.Equal(t, task1, task)
}

func TestConcurrency(t *testing.T) {
	scheduler := scheduler.NewWFQScheduler()

	q1 := queue.NewQueue(10)
	q2 := queue.NewQueue(20)
	scheduler.AddQueue(q1)
	scheduler.AddQueue(q2)

	task1 := &queue.Task{ID: ulid.Make(), Weight: 1, ExecTime: 1 * time.Second}
	task2 := &queue.Task{ID: ulid.Make(), Weight: 1, ExecTime: 1 * time.Second}
	q1.AddTask(task1)
	q2.AddTask(task2)

	done := make(chan bool)

	go func() {
		scheduler.UpdateVirtualTime(time.Now())
		task := scheduler.GetNextTask()
		assert.Equal(t, task1, task)
		done <- true
	}()

	go func() {
		scheduler.UpdateVirtualTime(time.Now())
		task := scheduler.GetNextTask()
		assert.Equal(t, task2, task)
		done <- true
	}()

	<-done
	<-done
}

func TestUpdateVirtualTime(t *testing.T) {
	scheduler := scheduler.NewWFQScheduler()
	newTime := time.Now().Add(10 * time.Minute)
	scheduler.UpdateVirtualTime(newTime)
	assert.Equal(t, newTime, scheduler.GetVirtualTime())
}

func TestSchedule(t *testing.T) {
	scheduler := scheduler.NewWFQScheduler()

	q1 := queue.NewQueue(10)
	scheduler.AddQueue(q1)

	task1 := &queue.Task{ID: ulid.Make(),
		Weight:    1,
		ExecTime:  1 * time.Second,
		Timestamp: time.Now(),
	}
	q1.AddTask(task1)

	tasks := q1.GetTasks()
	require.Lenf(t, tasks, 1, "Expected 1 task, got %d", len(tasks))
	assert.Equalf(t, task1, tasks[0], "Expected task %v, got %v", task1, tasks[0])

	scheduler.UpdateVirtualTime(time.Now())
	task := scheduler.Schedule()

	assert.Equalf(t, task1, task, "Expected task %v after scheduling, got %v", task1, task)
	expectedVirtualTime := task1.Timestamp.Add(task1.ExecTime)
	assert.Equalf(t, expectedVirtualTime, scheduler.GetVirtualTime(), "Expected virtual time %v, got %v", expectedVirtualTime, scheduler.GetVirtualTime())
	assert.Equalf(t, 0, len(q1.GetTasks()), "Expected 0 tasks in queue, got %d", len(q1.GetTasks()))
}
