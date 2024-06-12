package queue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mjholub/wq/internal/queue"
	"github.com/mjholub/wq/internal/scheduler"
)

func TestNewQueue(t *testing.T) {
	q := queue.NewQueue(10)
	require.NotNil(t, q)
	assert.Equal(t, 10, q.GetWeight())
	assert.Empty(t, q.GetTasks())
	assert.Implements(t, (*scheduler.WFQSchedulable)(nil), q)
}

func TestAddTask(t *testing.T) {
	q := queue.NewQueue(10)
	task := &queue.Task{}
	q.AddTask(task)
	assert.Len(t, q.GetTasks(), 1)
	assert.Equal(t, task, q.GetTasks()[0])
}

func TestPop(t *testing.T) {
	q := queue.NewQueue(10)
	task := &queue.Task{}
	q.AddTask(task)
	popped := q.Pop()
	assert.Equal(t, task, popped)
	assert.Empty(t, q.GetTasks())
}

func TestConcurrentPop(t *testing.T) {
	q := queue.NewQueue(10)
	task := &queue.Task{}
	q.AddTask(task)

	popped := make(chan *queue.Task)
	go func() {
		popped <- q.Pop()
	}()

	go func() {
		popped <- q.Pop()
	}()

	assert.Len(t, q.GetTasks(), 0)
	assert.Len(t, popped, 2)
}
