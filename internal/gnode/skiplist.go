package gnode

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

var (
	ErrEmpty = errors.New("skiplist is empty")
)

type skiplist struct {
	name  string
	ch    chan *Job
	ctx   *Context
	rand  *rand.Rand
	head  *skiplistNode // header point
	size  int
	level int
	// exitChan chan struct{}
}

type skiplistNode struct {
	score    int
	value    *Job
	forwards []*skiplistNode // forward points
}

func NewSkiplist(ctx *Context, name string) *skiplist {
	sl := &skiplist{}
	sl.level = 32
	sl.ch = make(chan *Job)
	sl.ctx = ctx
	sl.name = name
	sl.head = &skiplistNode{forwards: make([]*skiplistNode, 32)}
	sl.rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	return sl
}

// search by score
func (s *skiplist) Search(score int) *Job {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for {
			if x.forwards[i] != nil && x.forwards[i].score < score {
				x = x.forwards[i]
			} else {
				break
			}
		}
	}

	x = x.forwards[0]
	if x != nil && x.score == score {
		return x.value
	} else {
		return nil
	}
}

func (s *skiplist) PopByJobId(jobId int64) *Job {
	var target *skiplistNode
	x := s.head.forwards[0]
	for x != nil {
		if x.value.Id == jobId {
			target = x
			break
		}
		x = x.forwards[0]
	}
	if target == nil {
		return nil
	}

	// reset pointer
	var updates = make(map[int]*skiplistNode)
	x = s.head
	for i := s.level - 1; i >= 0; i-- {
		for {
			if x.forwards[i] != nil && x.forwards[i].score < target.score {
				x = x.forwards[i]
			} else {
				break
			}
		}
		updates[i] = x
	}

	for i, _ := range target.forwards {
		x = updates[i]
		x.forwards[i] = target.forwards[i]
	}

	s.size--
	return target.value
}

func (s *skiplist) Insert(value *Job, score int) bool {
	var updates = make(map[int]*skiplistNode)
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for {
			if x.forwards[i] != nil && x.forwards[i].score < score {
				x = x.forwards[i]
			} else {
				break
			}
		}
		updates[i] = x
	}

	lvl := s.randomLevel()
	newNode := &skiplistNode{score, value, make([]*skiplistNode, lvl)}
	for i := lvl - 1; i >= 0; i-- {
		x = updates[i]
		newNode.forwards[i] = x.forwards[i]
		x.forwards[i] = newNode
	}

	s.size++
	updates = nil
	return true
}

// remove frist node
func (s *skiplist) Shift() (*Job, error) {
	if s.size == 0 {
		return nil, ErrEmpty
	}

	x := s.head.forwards[0]
	for k, v := range x.forwards {
		s.head.forwards[k] = v
	}

	s.size--
	return x.value, nil
}

// remove arrtival node
func (s *skiplist) Arrival(score int) (*Job, error) {
	if s.size == 0 {
		return nil, ErrEmpty
	}
	if s.head.forwards[0] == nil {
		return nil, ErrEmpty
	}

	x := s.head.forwards[0]
	if x.score > score {
		return nil, nil
	}

	for k, v := range x.forwards {
		s.head.forwards[k] = v
	}

	s.size--
	return x.value, nil
}

func (s *skiplist) Size() int {
	return s.size
}

func (s *skiplist) PrintList() {
	x := s.head.forwards[0]
	for x != nil {
		// fmt.Println(fmt.Sprintf("score:%v,value:%v", x.score, x.value))
		x = x.forwards[0]
	}
	fmt.Println(fmt.Sprintf("%s total:%v", s.name, s.size))
}

func (s *skiplist) randomLevel() int {
	lvl := 1
	for s.rand.Intn(2) == 1 && lvl <= s.level {
		lvl++
	}
	return lvl
}
