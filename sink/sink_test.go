package main

import (
	"sync/atomic"
	"testing"
)

func BenchmarkRingPush(b *testing.B) {
	r := newRing(128)
	n := 0
	for i := 0; i < b.N; i++ {
		if n == r.cap {
			r.Reset()
			n = 0
		}
		r.PushCounter("foo", uint64(i))
	}
}

func BenchmarkRingPushBetterConsume(b *testing.B) {
	r := newRing(128)
	count := new(int64)
	go r.BetterConsume(func(Stat) { atomic.AddInt64(count, 1) })
	n := 0
	for i := 0; i < b.N; i++ {
		if n == r.cap {
			r.Reset()
			n = 0
		}
		r.PushCounter("foo", uint64(i))
	}
	b.Logf("Count: %d", atomic.LoadInt64(count))
}

func BenchmarkRingPushConsume(b *testing.B) {
	r := newRing(128)
	count := new(int64)
	go r.Consume(func(Stat) { atomic.AddInt64(count, 1) })
	n := 0
	for i := 0; i < b.N; i++ {
		if n == r.cap {
			r.Reset()
			n = 0
		}
		r.PushCounter("foo", uint64(i))
	}
	b.Logf("Count: %d", atomic.LoadInt64(count))
}
