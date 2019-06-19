package stats

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	logger "github.com/sirupsen/logrus"
)

// TODO(btc): add constructor that accepts functional options in order to allow
// users to choose the constants that work best for them. (Leave the existing
// c'tor for backwards compatibility)
// e.g. `func NewTCPStatsdSinkWithOptions(opts ...Option) Sink`

const (
	flushInterval           = time.Second
	logOnEveryNDroppedBytes = 1 << 15 // Log once per 32kb of dropped stats
	defaultBufferSize       = 1 << 16
	approxMaxMemBytes       = 1 << 22
	chanSize                = approxMaxMemBytes / defaultBufferSize
)

// NewTCPStatsdSink returns a FlushableSink that is backed by a buffered writer
// and a separate goroutine that flushes those buffers to a statsd connection.
func NewTCPStatsdSink() FlushableSink {
	outc := make(chan *bytes.Buffer, chanSize) // TODO(btc): parameterize
	writer := sinkWriter{
		outc: outc,
	}
	s := &tcpStatsdSink{
		outc:      outc,
		bufWriter: bufio.NewWriterSize(&writer, defaultBufferSize), // TODO(btc): parameterize size
	}
	s.flushCond = sync.NewCond(&s.mu)
	go s.run()
	return s
}

type tcpStatsdSink struct {
	conn net.Conn
	outc chan *bytes.Buffer

	mu            sync.Mutex
	droppedBytes  uint64
	bufWriter     *bufio.Writer
	flushCond     *sync.Cond
	lastFlushTime time.Time
}

type sinkWriter struct {
	outc chan<- *bytes.Buffer
}

func (w *sinkWriter) Write(p []byte) (int, error) {
	n := len(p)
	dest := getBuffer()
	dest.Write(p)
	select {
	case w.outc <- dest:
		return n, nil
	default:
		return 0, fmt.Errorf("statsd channel full, dropping stats buffer with %d bytes", n)
	}
}

func (s *tcpStatsdSink) Flush() {
	now := time.Now()
	if err := s.flush(); err != nil {
		// Not much we can do here; we don't know how/why we failed.
		return
	}
	s.mu.Lock()
	for now.After(s.lastFlushTime) {
		s.flushCond.Wait()
	}
	s.mu.Unlock()
}

func (s *tcpStatsdSink) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.bufWriter.Flush()
	if err != nil {
		s.handleFlushError(err, s.bufWriter.Buffered())
		return err
	}
	return nil
}

// s.mu should be held
func (s *tcpStatsdSink) handleFlushError(err error, droppedBytes int) {
	d := uint64(droppedBytes)
	if (s.droppedBytes+d)%logOnEveryNDroppedBytes > s.droppedBytes%logOnEveryNDroppedBytes {
		logger.WithField("total_dropped_bytes", s.droppedBytes+d).
			WithField("dropped_bytes", d).
			Error(err)
	}
	s.droppedBytes += d

	s.bufWriter.Reset(&sinkWriter{
		outc: s.outc,
	})
}

func (s *tcpStatsdSink) flushUint64(name, suffix string, u uint64) {
	b := pbFree.Get().(*buffer)

	b.WriteString(name)
	b.WriteChar(':')
	b.WriteUnit64(u)
	b.WriteString(suffix)

	s.mu.Lock()
	if _, err := s.bufWriter.Write(*b); err != nil {
		s.handleFlushError(err, s.bufWriter.Buffered())
	}
	s.mu.Unlock()

	b.Reset()
	pbFree.Put(b)
}

func (s *tcpStatsdSink) flushFloat64(name, suffix string, f float64) {
	b := pbFree.Get().(*buffer)

	b.WriteString(name)
	b.WriteChar(':')
	b.WriteFloat64(f)
	b.WriteString(suffix)

	s.mu.Lock()
	if _, err := s.bufWriter.Write(*b); err != nil {
		s.handleFlushError(err, s.bufWriter.Buffered())
	}
	s.mu.Unlock()

	b.Reset()
	pbFree.Put(b)
}

func (s *tcpStatsdSink) FlushCounter(name string, value uint64) {
	s.flushUint64(name, "|c\n", value)
}

func (s *tcpStatsdSink) FlushGauge(name string, value uint64) {
	s.flushUint64(name, "|g\n", value)
}

func (s *tcpStatsdSink) FlushTimer(name string, value float64) {
	// Since we mistakenly use floating point values to represent time
	// durations this method is often passed an integer encoded as a
	// float. Formatting integers is much faster (>2x) than formatting
	// floats so use integer formatting whenever possible.
	//
	if 0 <= value && value < math.MaxUint64 && math.Trunc(value) == value {
		s.flushUint64(name, "|ms\n", uint64(value))
	} else {
		s.flushFloat64(name, "|ms\n", value)
	}
}

func (s *tcpStatsdSink) run() {
	settings := GetSettings()
	t := time.NewTicker(flushInterval)
	defer t.Stop()
	for {
		if s.conn == nil {
			conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", settings.StatsdHost,
				settings.StatsdPort))
			if err != nil {
				logger.Warnf("statsd connection error: %s", err)
				time.Sleep(3 * time.Second)
				continue
			}
			s.conn = conn
		}

		select {
		case <-t.C:
			s.flush()
		case buf, ok := <-s.outc: // Receive from the channel and check if the channel has been closed
			if !ok {
				logger.Warnf("Closing statsd client")
				s.conn.Close()
				return
			}
			lenbuf := len(buf.Bytes())
			_, err := buf.WriteTo(s.conn)
			if len(s.outc) == 0 {
				// We've at least tried to write all the data we have. Wake up anyone waiting on flush.
				s.mu.Lock()
				s.lastFlushTime = time.Now()
				s.mu.Unlock()
				s.flushCond.Broadcast()
			}
			if err != nil {
				s.mu.Lock()
				s.handleFlushError(err, lenbuf)
				s.mu.Unlock()
				_ = s.conn.Close() // Ignore close failures
				s.conn = nil
			}
			putBuffer(buf)
		}
	}
}

//////////////////////////////////////////////////////////////////

type timeoutWriter struct {
	timeout time.Duration
	conn    net.Conn
	err     error
}

func (w *timeoutWriter) Write(b []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	// CEV: ignore deadline errors as there is nothing we
	// can do about them.
	w.conn.SetWriteDeadline(time.Now().Add(w.timeout))
	var n int
	n, w.err = w.conn.Write(b)
	w.conn.SetWriteDeadline(time.Time{})
	return n, w.err
}

// func (w *timeoutWriter) Close() (err error) {
// 	if w.conn != nil {
// 		err = w.conn.Close()
// 		w.conn = nil
// 	}
// 	return err
// }

//////////////////////////////////////////////////////////////////

const maxRetries = 10

// TODO: add optional logger
// TODO: consider setting write deadline
type netWriter struct {
	conn    net.Conn
	buf     *bufio.Writer
	mu      sync.Mutex
	network string
	raddr   string

	droppedBytes int64
	writeTimeout time.Duration
}

// TODO: maybe rename to 'dial'
func newNetWriter(network, raddr string) (*netWriter, error) {
	w := &netWriter{
		network: network,
		raddr:   raddr,
	}
	if err := w.connect(); err != nil {
		return nil, err
	}
	return w, nil
}

const (
	DialTimeout  = time.Second
	WriteTimeout = time.Millisecond * 500
)

func (w *netWriter) connect() error {
	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}
	c, err := net.DialTimeout(w.network, w.raddr, DialTimeout)
	if err != nil {
		return err
	}
	w.conn = c

	var wr io.Writer = w.conn
	if w.writeTimeout > 0 {
		wr = &timeoutWriter{
			conn:    w.conn,
			timeout: w.writeTimeout,
		}
	}
	if w.buf == nil {
		w.buf = bufio.NewWriterSize(nil, defaultBufferSize)
	}
	w.buf = bufio.NewWriterSize(wr, defaultBufferSize)

	return nil
}

func (w *netWriter) Close() (err error) {
	w.mu.Lock()
	if w.conn != nil {
		err = w.buf.Flush()
		if ec := w.conn.Close(); ec != nil && err == nil {
			err = ec
		}
		w.conn = nil
	}
	w.mu.Unlock()
	return
}

func (w *netWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	n, err := w.writeAndRetry(b, 0)
	w.mu.Unlock()
	return n, err
}

func (w *netWriter) writeAndRetry(b []byte, retryCount int) (n int, err error) {
	if w.conn != nil {
		if n, err = w.buf.Write(b); err == nil {
			// TODO: handle partial writes which
			// may occur if there is a timeout
			return
		}
		// prevent an infinite retry loop
		retryCount++
		if retryCount >= maxRetries {
			return
		}
	}
	// no sleep on the first retry
	if retryCount > 1 {
		time.Sleep(time.Second * 3)
	}
	if err = w.connect(); err != nil {
		return
	}
	return w.writeAndRetry(b, retryCount)
}

//////////////////////////////////////////////////////////////

type statType uint

const (
	counterStat = statType(iota)
	gaugeStat
	timerStat
)

// WARN: remove if unused
var ErrInvalidStatType = errors.New("invalid stat type")

func (s statType) Valid() bool { return s <= timerStat }

// TODO: add WriteTo() method
type stat struct {
	Name  string
	Value uint64
	Typ   statType // TODO: make first struct field
}

func (s *stat) WriteTo(w io.Writer) (int64, error) {
	if !s.Typ.Valid() {
		return 0, ErrInvalidStatType
	}

	b := pbFree.Get().(*buffer)

	b.WriteString(s.Name)
	b.WriteChar(':')

	switch s.Typ {
	case counterStat:
		b.WriteUnit64(s.Value)
		b.WriteString("|c\n")
	case gaugeStat:
		b.WriteUnit64(s.Value)
		b.WriteString("|g\n")
	case timerStat:
		// Since we mistakenly use floating point values to represent time
		// durations this method is often passed an integer encoded as a
		// float. Formatting integers is much faster (>2x) than formatting
		// floats so use integer formatting whenever possible.
		//
		val := math.Float64frombits(s.Value)
		if 0 <= val && val < math.MaxUint64 && math.Trunc(val) == val {
			b.WriteUnit64(uint64(val))
		} else {
			b.WriteFloat64(val)
		}
		b.WriteString("|ms\n")
	}

	n, err := w.Write(*b)
	b.Reset() // TODO: make reset return b
	pbFree.Put(b)

	return int64(n), err
}

// element is an element of a linked list of stats
type element struct {
	next *element
	stat stat
}

// newElementList returns a linked list of elements.
func newElementList(n int) *element {
	if n <= 0 {
		panic("stats: non-positive size for newElementList")
	}
	a := make([]element, n) // optimize locality
	head := &a[0]
	p := head
	for i := 1; i < len(a); i++ {
		p.next = &a[i]
		p = p.next
	}
	p.next = head
	return head
}

// TODO: rename to make it clear that this is actually a queue?
type ring struct {
	mu   sync.Mutex
	cond sync.Cond

	// WARN WARN WARN
	w io.Writer // WARN WARN WARN
	// WARN WARN WARN

	len     int      // number of buffered stats
	cap     int      // total stat capacity
	head    *element // we consume from head
	tail    *element // we add to tail
	root    *element // root node
	dropped int      // dropped stats
}

func newRing(n int) *ring {
	if n <= 0 {
		panic("ring: non-positive capacity")
	}
	root := newElementList(n)
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

func (r *ring) push(name string, value uint64, typ statType) {
	r.mu.Lock()
	if r.len < r.cap {
		r.len++
		r.tail.stat = stat{
			Name:  name,
			Value: value,
			Typ:   typ,
		}
		r.tail = r.tail.next
	} else {
		r.dropped++
	}
	r.mu.Unlock()
	r.cond.Signal()
}

func (r *ring) FlushCounter(name string, value uint64) {
	r.push(name, value, counterStat)
}

func (r *ring) FlushGauge(name string, value uint64) {
	r.push(name, value, gaugeStat)
}

func (r *ring) FlushTimer(name string, value float64) {
	r.push(name, math.Float64bits(value), timerStat)
}

// TODO: only flush if stats are buffered
func (r *ring) Flush() { r.cond.Signal() }

//////////////////////////////////////////////////////////////

var bufferPool sync.Pool

func getBuffer() *bytes.Buffer {
	if v := bufferPool.Get(); v != nil {
		b := v.(*bytes.Buffer)
		b.Reset()
		return b
	}
	return new(bytes.Buffer)
}

func putBuffer(b *bytes.Buffer) {
	bufferPool.Put(b)
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

// TODO: change this to return a pointer to the buffer
func (b *buffer) Reset() { *b = (*b)[:0] }

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

func (b *buffer) WriteUnit64(val uint64) {
	*b = strconv.AppendUint(*b, val, 10)
}

func (b *buffer) WriteFloat64(val float64) {
	*b = strconv.AppendFloat(*b, val, 'f', 6, 64)
}
