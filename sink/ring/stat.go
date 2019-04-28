package main

import (
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
)

type StatType uint

const (
	CounterStat = StatType(iota)
	GaugeStat
	TimerStat
)

var ErrInvalidStatType = errors.New("invalid stat type")

// TODO: add WriteTo() method
type Stat struct {
	Name  string
	Value uint64
	Typ   StatType // TODO: make first struct field
}

func (s *Stat) valid() bool { return s.Typ <= TimerStat }

func (s Stat) String() string {
	var b strings.Builder
	if s.valid() {
		s.WriteTo(&b)
		return b.String()
	}
	return ErrInvalidStatType.Error()
}

func (s *Stat) WriteTo(w io.Writer) (int64, error) {
	if !s.valid() {
		return 0, ErrInvalidStatType
	}

	b := pbFree.Get().(*buffer)

	b.WriteString(s.Name)
	b.WriteChar(':')
	switch s.Typ {
	case CounterStat:
		b.WriteUnit64(s.Value)
		b.WriteString("|c\n")
	case GaugeStat:
		b.WriteUnit64(s.Value)
		b.WriteString("|g\n")
	case TimerStat:
		b.WriteFloat64(math.Float64frombits(s.Value))
		b.WriteString("|ms\n")
	default:
		panic("unreachable")
	}

	n, err := w.Write(*b)
	pbFree.Put(b.Reset())

	return int64(n), err
}

// pbFree is the print buffer pool
var pbFree = sync.Pool{
	New: func() interface{} {
		b := make(buffer, 0, 128)
		return &b
	},
}

// Use a fast and simple buffer for constructing statsd messages
type buffer []byte

func (b *buffer) Reset() *buffer {
	*b = (*b)[:0]
	return b
}

func (b *buffer) Write(p []byte) {
	*b = append(*b, p...)
}

func (b *buffer) WriteString(s string) {
	*b = append(*b, s...)
}

// This is named WriteChar instead of WriteByte because the 'stdmethods' check
// of 'go vet' wants WriteByte to have the signature:
//
// 	func (b *buffer) WriteByte(c byte) error { ... }
//
func (b *buffer) WriteChar(c byte) {
	*b = append(*b, c)
}

// small returns the string for an i with 0 <= i < nSmalls.
func small(i uint64) string {
	const digits = "0123456789"
	if i < 10 {
		return digits[i : i+1]
	}
	return smallsString[i*2 : i*2+2]
}

const nSmalls = 100

const smallsString = "00010203040506070809" +
	"10111213141516171819" +
	"20212223242526272829" +
	"30313233343536373839" +
	"40414243444546474849" +
	"50515253545556575859" +
	"60616263646566676869" +
	"70717273747576777879" +
	"80818283848586878889" +
	"90919293949596979899"

func (b *buffer) WriteUnit64(u uint64) {
	if 0 <= u && u < nSmalls {
		b.WriteString(small(u))
		return
	}

	var a [20]byte
	i := len(a)

	for u >= 100 {
		is := u % 100 * 2
		u /= 100
		i -= 2
		a[i+1] = smallsString[is+1]
		a[i+0] = smallsString[is+0]
	}

	// u < 100
	is := u * 2
	i--
	a[i] = smallsString[is+1]
	if u >= 10 {
		i--
		a[i] = smallsString[is]
	}

	*b = append(*b, a[i:]...)
}

func (b *buffer) WriteFloat64(val float64) {
	if 0 <= val && val < math.MaxUint64 && math.Trunc(val) == val {
		b.WriteUnit64(uint64(val))
	} else {
		*b = strconv.AppendFloat(*b, val, 'f', 6, 64)
	}
}
