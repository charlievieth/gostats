package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"sync"
	"time"
)

type StatType int

const (
	counterStat = StatType(iota)
	gaugeStat
	timerStat
)

type Stat struct {
	Name  string
	Value uint64
	Typ   StatType // TODO: make first struct field
}

func (s Stat) String() string {
	switch s.Typ {
	case counterStat:
		return fmt.Sprintf("%s:%d|c", s.Name, s.Value)
	case gaugeStat:
		return fmt.Sprintf("%s:%d|g", s.Name, s.Value)
	case timerStat:
		return fmt.Sprintf("%s:%f|ms", s.Name, math.Float64frombits(s.Value))
	default:
		return fmt.Sprintf("invlaid stat type: %d", s.Typ)
	}
}

type Element struct {
	next *Element
	Stat Stat
}

// TODO: consider allowing f() to stop iteration by returning a bool
func (e *Element) Do(f func(st Stat)) {
	if e != nil && e.next != nil {
		f(e.Stat)
		for p := e.next; p != e; p = p.next {
			f(p.Stat)
		}
	}
}

func (e *Element) Clear() {
	if e != nil && e.next != nil {
		last := e
		for p := e.next; p != e; p = p.next {
			*last = Element{}
			last = p
		}
		*last = Element{}
	}
}

// TODO: rename
func newList(n int) *Element {
	a := make([]Element, n) // optimize locality
	r := &a[0]
	p := r
	for i := 1; i < n; i++ {
		p.next = &a[i]
		p = p.next
	}
	p.next = r
	return r
}

// func (s *stat) Empty() bool { return *s == stat{} }
// func (s *stat) Clear()      { *s = stat{} }

// func (l *list) Len() int {
// 	n := 0
// 	if l != nil {
// 		n = 1
// 		if l.next != nil {
// 			for p := l.next; p != l; p = p.next {
// 				n++
// 			}
// 		}
// 	}
// 	return n
// }

type ring struct {
	mu   sync.Mutex
	cond sync.Cond

	len  int
	cap  int
	head *Element // we consume from head
	tail *Element // we add to tail
	root *Element // root node
}

func newRing(n int) *ring {
	root := newList(n)
	r := &ring{
		root: root,
		head: root,
		tail: root,
		cap:  n,
	}
	r.cond.L = &r.mu
	return r
}

func (r *ring) Len() int {
	r.mu.Lock()
	n := r.len
	r.mu.Unlock()
	return n
}

func (r *ring) Cap() int {
	r.mu.Lock()
	n := r.cap
	r.mu.Unlock()
	return n
}

func (r *ring) do(f func(st Stat)) {
	if r != nil && r.root != nil {
		p := r.root
		for i := 0; i < r.len; i++ {
			f(p.Stat)
			p = p.next
		}
	}
}

func (r *ring) Do(f func(st Stat)) {
	r.mu.Lock()
	r.do(f)
	r.mu.Unlock()
}

// CEV: this is for testing
func (r *ring) Reset() {
	r.mu.Lock()
	r.head = r.root
	r.tail = r.root
	r.len = 0
	r.mu.Unlock()
}

// TODO: cap/limit size ???
func (r *ring) grow() {
	// lock must be held
	if r.cap == 0 {
		r.cap = 32 // TODO: use a const
	}
	cap := r.cap * 2
	root := newList(cap)

	// copy the old list moving head to the new root node
	tail := root
	p := r.head
	for i := 0; i < r.len; i++ {
		tail.Stat = p.Stat
		tail = tail.next
		p = p.next
	}
	// TODO: do we actually care about clearing old entries?
	r.root.Clear()

	r.root = root
	r.head = root
	r.tail = tail
	r.cap = cap
}

func (r *ring) push(st Stat) {
	r.mu.Lock()
	if r.len == r.cap || r.cap == 0 {
		r.grow()
	}
	r.tail.Stat = st
	r.tail = r.tail.next
	r.len++
	r.mu.Unlock()
	r.cond.Signal()
}

func (r *ring) pop() (p *Element) {
	if r.len > 0 {
		p = r.head
		r.head = p.next
		r.len--
	}
	return p
}

// WARN: we can probably delete this
func (r *ring) Pop() (Stat, bool) {
	r.mu.Lock()
	p := r.pop()
	r.mu.Unlock()
	if p != nil {
		return p.Stat, true
	}
	return Stat{}, false
}

// TODO: keep only one of these
func (r *ring) Consume(fn func(st Stat)) {
	for {
		r.mu.Lock()
		for r.len == 0 {
			r.cond.Wait()
		}
		for ; r.len > 0; r.len-- {
			fn(r.head.Stat) // WARN: this may block !!!
			r.head = r.head.next
		}
		r.mu.Unlock()
	}
}

func (r *ring) BetterConsume(fn func(st Stat)) {
	// WARN: need a sane minimum since r.cap may grow
	stats := make([]Stat, 0, r.cap)
	for {
		r.mu.Lock()
		for r.len == 0 {
			r.cond.Wait()
		}
		// consume as many stats as we can while we have
		// the lock
		stats := stats[:0]
		for ; r.len > 0 && len(stats) < cap(stats); r.len-- {
			stats = append(stats, r.head.Stat)
			r.head = r.head.next
		}
		r.mu.Unlock()
		// call fn outside the lock
		for i := range stats {
			fn(stats[i])
		}
	}
}

// TODO: keep only one of these
func (r *ring) Watch(fn func(st Stat)) {
	r.mu.Lock()
	for r.len == 0 {
		r.cond.Wait()
	}
	r.do(fn)
	r.mu.Unlock()
}

func (r *ring) PushCounter(name string, value uint64) {
	r.push(Stat{Name: name, Value: value, Typ: counterStat})
}

func (r *ring) PushGauge(name string, value uint64) {
	r.push(Stat{Name: name, Value: value, Typ: gaugeStat})
}

func (r *ring) PushTimer(name string, value float64) {
	r.push(Stat{
		Name:  name,
		Value: math.Float64bits(value),
		Typ:   timerStat,
	})
}

type Conn struct {
	queue []byte
	bw    *bufio.Writer
	w     io.Writer
}

func (c *Conn) Write(p []byte) (int, error) {
	return -1, nil
}

func main() {
	r := newRing(5)
	go r.Consume(func(st Stat) {
		fmt.Printf("Watch: %s\n", st.String())
	})
	for i := 0; i < 10; i++ {
		r.push(Stat{Name: fmt.Sprintf("s_%d", i)})
		if r.len == 5 {
			n := 0
			r.Do(func(st Stat) {
				fmt.Printf("Do %d - %s\n", n, st.Name)
				n++
			})
		}
	}
	fmt.Println("len:", r.len)
	// for i := 0; i < 20; i++ {
	// 	st, ok := r.Pop()
	// 	if ok {
	// 		fmt.Printf("%d - %d: %+v %s\n", i, r.len, p, p.Stat.Name)
	// 	} else {
	// 		fmt.Printf("%d - %d: %+v\n", i, r.len, p)
	// 	}
	// }
	time.Sleep(time.Millisecond * 20)
}
