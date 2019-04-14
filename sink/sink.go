package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
)

type statType int

const (
	counterStat = statType(iota)
	gaugeStat
	timerStat
)

type stat struct {
	name  string
	value uint64
	typ   statType // TODO: make first struct field
}

func (s stat) String() string {
	switch s.typ {
	case counterStat:
		return fmt.Sprintf("%s:%d|c", s.name, s.value)
	case gaugeStat:
		return fmt.Sprintf("%s:%d|g", s.name, s.value)
	case timerStat:
		return fmt.Sprintf("%s:%f|ms", s.name, math.Float64frombits(s.value))
	default:
		return fmt.Sprintf("invlaid stat type: %d", s.typ)
	}
}

type list struct {
	next *list
	stat stat
}

func (l *list) Do(f func(st stat)) {
	if l != nil && l.next != nil {
		f(l.stat)
		for p := l.next; p != l; p = l.next {
			f(p.stat)
		}
	}
}

func newList(n int) *list {
	a := make([]list, n) // optimize locality
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
	len  int
	cap  int
	head *list // we consume from head
	tail *list // we add to tail
	root *list // root node
}

func newRing(n int) *ring {
	r := newList(n)
	return &ring{
		root: r,
		head: r,
		tail: r,
		cap:  n,
	}
}

func (r *ring) Do(f func(st stat)) {
	if r != nil && r.root != nil {
		p := r.root
		for i := 0; i < r.len; i++ {
			f(p.stat)
			p = p.next
		}
	}
}

// TODO: cap size ???
func (r *ring) grow() {
	if r.cap == 0 {
		r.cap = 32 // TODO: use a const
	}
	cap := r.cap * 2
	root := newList(cap)

	// copy the old list moving head to the new root node
	tail := root
	p := r.head
	for i := 0; i < r.len; i++ {
		tail.stat = p.stat
		tail = tail.next
		p = p.next
	}

	r.root = root
	r.head = root
	r.tail = tail
	r.cap = cap
}

func (r *ring) push(st stat) {
	if r.len == r.cap || r.cap == 0 {
		r.grow()
	}
	r.tail.stat = st
	r.tail = r.tail.next
	r.len++
}

func (r *ring) pop() (p *list) {
	if r.len > 0 {
		p = r.head
		r.head = p.next
		r.len--
	}
	return p
}

func (r *ring) PushCounter(name string, value uint64) {
	r.push(stat{name: name, value: value, typ: counterStat})
}

func (r *ring) PushGauge(name string, value uint64) {
	r.push(stat{name: name, value: value, typ: gaugeStat})
}

func (r *ring) PushTimer(name string, value float64) {
	r.push(stat{
		name:  name,
		value: math.Float64bits(value),
		typ:   timerStat,
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
	for i := 0; i < 10; i++ {
		r.push(stat{name: fmt.Sprintf("s_%d", i)})
		if r.len == 5 {
			n := 0
			r.Do(func(st stat) {
				fmt.Printf("Do %d - %s\n", n, st.name)
				n++
			})
		}
	}
	fmt.Println("len:", r.len)
	for i := 0; i < 20; i++ {
		p := r.pop()
		if p == nil {
			fmt.Printf("%d - %d: %+v\n", i, r.len, p)
		} else {
			fmt.Printf("%d - %d: %+v %s\n", i, r.len, p, p.stat.name)
		}
	}
}
