package gnode

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrLikedListEmpty = errors.New("likedList is empty")
	ErrLikeListPos    = errors.New("pos can't not greater than length of likedList")
)

type Node struct {
	Data *Job
	Prev *Node
	Next *Node
}

type LikedList struct {
	Size        int
	Front, Back *Node
	ReadyChan   chan *Job
	waitChan    chan struct{}
	exitChan    chan struct{}
	ctx         *Context
	sync.Mutex
}

func (l *LikedList) IsEmpty() bool {
	return l.Size == 0
}

func NewLikedList(ctx *Context) *LikedList {
	n := &Node{
		Data: nil,
		Prev: nil,
		Next: nil,
	}
	l := &LikedList{
		Size:      0,
		Front:     n,
		Back:      n,
		ctx:       ctx,
		ReadyChan: make(chan *Job),
		waitChan:  make(chan struct{}),
		exitChan:  make(chan struct{}),
	}

	go l.timer()
	return l
}

func (l *LikedList) Push(data *Job) {
	l.Lock()
	defer l.Unlock()

	curNode := l.Back
	n := &Node{data, curNode, nil}

	curNode.Next = n
	l.Back = n

	if l.Front.Next == nil {
		l.Front.Next = n
	}

	l.Size++

	// waitChan: notify timer to start shifting the node
	select {
	case l.waitChan <- struct{}{}:
	default:
	}
}

func (l *LikedList) Unshift(data *Job) {
	node := &Node{
		Data: data,
		Prev: l.Front,
		Next: l.Front.Next,
	}

	if l.IsEmpty() {
		l.Back = node
	}

	l.Lock()
	nextNode := l.Front.Next
	l.Front.Next = node
	if nextNode != nil {
		nextNode.Prev = node
	}

	l.Size++
	l.Unlock()
}

func (l *LikedList) Insert(pos int, data *Job) error {
	if pos > 0 && l.IsEmpty() {
		return ErrLikedListEmpty
	}
	if pos > l.Size {
		return ErrLikeListPos
	}
	if pos == l.Size {
		l.Push(data)
		return nil
	}

	i := 0
	curNode := l.Front.Next
	for curNode != nil {
		if i == pos {
			node := &Node{
				Data: data,
				Prev: curNode.Prev,
				Next: curNode,
			}
			prevNode := curNode.Prev
			if prevNode != nil {
				prevNode.Next = node
			}
			curNode.Prev = node
			break
		}
		i++
		curNode = curNode.Next
	}

	return nil
}

// remove node from tail
func (l *LikedList) Pop() (*Job, error) {
	if l.IsEmpty() {
		return nil, ErrLikedListEmpty
	}

	n := l.Back
	p := l.Back.Prev
	p.Next = nil
	l.Back = p
	l.Size--
	return n.Data, nil
}

// remove node from head
func (l *LikedList) shift() (*Job, error) {
	l.Lock()
	defer l.Unlock()

	if l.Size == 0 {
		return nil, ErrLikedListEmpty
	}

	firstNode := l.Front.Next
	nextNode := firstNode.Next
	l.Front.Next = nextNode
	l.Size--
	return firstNode.Data, nil
}

func (l *LikedList) timer() {
	for {
		j, err := l.shift() // note: one message may be lose when topic is persisted
		if j != nil && err == nil {
			select {
			case l.ReadyChan <- j:
			case <-l.exitChan:
				return
			}
		} else {
			select {
			case <-l.waitChan:
			case <-l.exitChan:
				return
			}
		}
	}
}

func (l *LikedList) exit() {
	close(l.exitChan)
}

func (l *LikedList) GetPos(pos int) (*Job, error) {
	if l.IsEmpty() {
		return nil, ErrLikedListEmpty
	}
	if l.Size <= pos {
		return nil, ErrLikeListPos
	}

	i := 0
	curNode := l.Front.Next
	for curNode != nil {
		if i == pos {
			prevNode := curNode.Prev
			nextNode := curNode.Next
			if prevNode != nil {
				prevNode.Next = nextNode
			}
			if nextNode != nil {
				nextNode.Prev = prevNode
			}
			return curNode.Data, nil
		}
		i++
		curNode = curNode.Next
	}

	return nil, nil
}

func (l *LikedList) Foreach() {
	cur := l.Front
	for cur.Next != nil {
		cur = cur.Next
		fmt.Println(cur)
	}
	fmt.Println(l.Size)
}

// get node by index, but don't remove
func (l *LikedList) Get(pos int) (*Job, error) {
	if pos > 0 && l.IsEmpty() {
		return nil, ErrLikedListEmpty
	}
	if pos >= l.Size {
		return nil, ErrLikeListPos
	}

	i := 0
	cur := l.Front
	for cur.Next != nil {
		if i == pos {
			return cur.Next.Data, nil
		}
		i++
		cur = cur.Next
	}

	return nil, nil
}

func (l *LikedList) RealSize() int {
	size := 0
	curNode := l.Front.Next
	for curNode != nil {
		size++
		curNode = curNode.Next
	}
	return size
}
