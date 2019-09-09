package gnode

import (
	"sync"
)

type ItemQueue struct {
	items []*JobCard
	sync.RWMutex
}

func NewItemQueue() *ItemQueue {
	return &ItemQueue{}
}

func (q *ItemQueue) Push(item *JobCard) {
	q.Lock()
	defer q.Unlock()

	q.items = append(q.items, item)
}

func (q *ItemQueue) Pop() *JobCard {
	q.Lock()
	defer q.Unlock()

	if q.IsEmpty() {
		return nil
	}

	item := q.items[0]
	q.items = q.items[1:]
	return item
}

func (q *ItemQueue) IsEmpty() bool {
	return len(q.items) == 0
}

func (q *ItemQueue) Size() int {
	return len(q.items)
}
