// 跳跃表
// https://blog.csdn.net/ict2014/article/details/17394259
package gnode

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
)

var (
	ErrEmpty = errors.New("skiplist is empty")
)

type skiplist struct {
	head  *skiplistNode
	size  int32 // the number of node.value
	level int
	sync.Mutex
}

type skiplistNode struct {
	score    uint64
	value    []interface{}
	forwards []*skiplistNode // 前进指针
}

func NewSkiplist(level int) *skiplist {
	if level <= 0 {
		level = 1 << 5 // 32
	}

	sl := &skiplist{}
	sl.level = level
	sl.head = &skiplistNode{forwards: make([]*skiplistNode, level, level)}

	return sl
}

func (s *skiplist) Search(score uint64) interface{} {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for {
			if x.forwards[i] != nil && x.forwards[i].score > score {
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
		return ""
	}
}

func (s *skiplist) Insert(value interface{}, score uint64) bool {
	s.Lock()
	defer s.Unlock()

	updates := make(map[int]*skiplistNode, s.level)
	defer func() {
		updates = nil
	}()

	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for {
			if x.forwards[i] != nil && x.forwards[i].score > score {
				x = x.forwards[i]
			} else {
				break
			}
		}
		updates[i] = x
	}

	// score has exist, append to node.value
	if nextNode := x.forwards[0]; nextNode != nil {
		if nextNode.score == score {
			nextNode.value = append(nextNode.value, value)
			atomic.AddInt32(&s.size, 1)
			return true
		}
	}

	lvl := s.randomLevel()
	newNode := &skiplistNode{
		score:    score,
		value:    make([]interface{}, 0),
		forwards: make([]*skiplistNode, lvl, lvl),
	}
	newNode.value = append(newNode.value, value)

	for i := lvl - 1; i >= 0; i-- {
		newNode.forwards[i] = updates[i].forwards[i]
		updates[i].forwards[i] = newNode
	}

	atomic.AddInt32(&s.size, 1)
	return true
}

// 移出第一个节点
func (s *skiplist) Shift() ([]interface{}, error) {
	s.Lock()
	defer s.Unlock()

	if s.size <= 0 {
		return nil, ErrEmpty
	}

	x := s.head.forwards[0]
	v := x.value
	for i := 0; i < len(x.forwards); i++ {
		s.head.forwards[i] = x.forwards[i]
	}

	atomic.AddInt32(&s.size, -int32(len(x.value)-1))
	x = nil
	return v, nil
}

// 清除
func (s *skiplist) Clear() {
	s.Lock()
	defer s.Unlock()

	for i := 0; i < s.Size(); i++ {
		s.Shift()
	}
}

func (s *skiplist) Exipre(score uint64) (uint64, []interface{}, error) {
	s.Lock()
	defer s.Unlock()

	if s.size <= 0 {
		return 0, nil, ErrEmpty
	}

	x := s.head.forwards[0]
	if x.score > score {
		return 0, nil, ErrEmpty
	}

	msgIds := x.value
	delayTime := x.score
	for i := 0; i < len(x.forwards); i++ {
		s.head.forwards[i] = x.forwards[i]
	}

	atomic.AddInt32(&s.size, -int32(len(x.value)))
	x = nil
	return delayTime, msgIds, nil
}

func (s *skiplist) Size() int {
	return int(atomic.LoadInt32(&s.size))
}

func (s *skiplist) PrintList() {
	x := s.head.forwards[0]
	for x != nil {
		fmt.Println(fmt.Sprintf("score:%v,value:%v", x.score, x.value))
		x = x.forwards[0]
	}
	fmt.Println(fmt.Sprintf("total:%v", s.size))
}

func (s *skiplist) randomLevel() int {
	lvl := 1
	for rand.Int()%2 == 1 && lvl <= s.level {
		lvl++
	}
	return lvl
}
