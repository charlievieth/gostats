package main

import (
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
)

type Element struct {
	next *Element
	Stat Stat
}

// TODO: consider allowing f() to stop iteration by returning a bool
// func (e *Element) Do(f func(st Stat)) {
// 	if e != nil && e.next != nil {
// 		f(e.Stat)
// 		for p := e.next; p != e; p = p.next {
// 			f(p.Stat)
// 		}
// 	}
// }

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

type ring struct {
	mu      sync.Mutex
	cond    sync.Cond
	len     int
	cap     int
	head    *Element // we consume from head
	tail    *Element // we add to tail
	root    *Element // root node
	dropped int      // dropped stats
}

func newRing(n int) *ring {
	if n <= 0 {
		panic("ring: non-positive size")
	}
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

func (r *ring) Dropped() int {
	r.mu.Lock()
	n := r.dropped
	r.mu.Unlock()
	return n
}

func (r *ring) push(st Stat) {
	r.mu.Lock()
	if r.len < r.cap {
		r.len++
		r.tail.Stat = st
		r.tail = r.tail.next
	} else {
		r.dropped++
	}
	r.mu.Unlock()
	r.cond.Signal()
}

func (r *ring) PushCounter(name string, value uint64) {
	r.push(Stat{Name: name, Value: value, Typ: CounterStat})
}

func (r *ring) PushGauge(name string, value uint64) {
	r.push(Stat{Name: name, Value: value, Typ: GaugeStat})
}

func (r *ring) PushTimer(name string, value float64) {
	r.push(Stat{Name: name, Value: math.Float64bits(value), Typ: TimerStat})
}

func (r *ring) Consume(fn func(st Stat)) {
	// TODO: use a smaller cap?
	stats := make([]Stat, 0, r.Cap())
	for {
		r.mu.Lock()
		for r.len == 0 {
			r.cond.Wait()
		}

		// WARN: this only works if cap(stats) == r.Cap()
		//
		// consume as many stats as we can while we have
		// the lock
		//
		stats = stats[:r.len]
		for i := range stats {
			stats[i] = r.head.Stat
			r.head = r.head.next
		}
		r.len = 0
		r.mu.Unlock()

		// WARN (CEV): make sure this doesn't block forever
		//
		// call fn outside the lock
		for i := range stats {
			fn(stats[i])
		}
	}
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

// CEV: this is for testing
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

func main() {
	addrs, _ := net.LookupHost("localhost")
	for _, s := range addrs {
		ip := net.ParseIP(s)
		// if err != nil {
		// 	fmt.Println("Error:", s, err)
		// }
		ipv4 := ip.To4()
		fmt.Println(ip, len(ip), ipv4, len(ipv4))
	}

	return

	s := strconv.FormatUint(math.MaxUint64, 10)
	fmt.Println(s, len(s))

	// r := newRing(5)
	// go r.Consume(func(st Stat) {
	// 	fmt.Printf("Watch: %s\n", st.String())
	// })
	// for i := 0; i < 10; i++ {
	// 	r.push(Stat{Name: fmt.Sprintf("s_%d", i)})
	// 	if r.len == 5 {
	// 		n := 0
	// 		r.Do(func(st Stat) {
	// 			fmt.Printf("Do %d - %s\n", n, st.Name)
	// 			n++
	// 		})
	// 	}
	// }
	// fmt.Println("len:", r.len)
	// // for i := 0; i < 20; i++ {
	// // 	st, ok := r.Pop()
	// // 	if ok {
	// // 		fmt.Printf("%d - %d: %+v %s\n", i, r.len, p, p.Stat.Name)
	// // 	} else {
	// // 		fmt.Printf("%d - %d: %+v\n", i, r.len, p)
	// // 	}
	// // }
	// time.Sleep(time.Millisecond * 20)
}
