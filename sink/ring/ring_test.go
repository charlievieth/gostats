package main

import (
	"sync/atomic"
	"testing"
)

func BenchmarkRingPushConsume(b *testing.B) {
	r := newRing(128)
	count := new(int64)
	go r.Consume(func(Stat) { atomic.AddInt64(count, 1) })
	go r.Consume(func(Stat) {})
	n := 0
	for i := 0; i < b.N; i++ {
		if n == r.cap {
			r.Reset()
			n = 0
		}
		r.PushCounter("foo", uint64(i))
	}
	if testing.Verbose() {
		n := atomic.LoadInt64(count)
		d := int64(r.Dropped())
		t := n + d
		var p float64
		if d != 0 {
			p = (float64(d) / float64(t)) * 100
		}
		b.Logf("Total: %d Sent: %d Dropped: %d%% - %.3f", t, n, d, p)
	}
}
