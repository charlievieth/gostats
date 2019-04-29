package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// TODO: tune and organize
const (
	// Optimize for networks with an MTU of 1500
	OptimalPayloadSize = 1432
	UDPMaxPayloadSize  = 65467

	// WARN: this conflicts with OptimalPayloadSize
	DefaultBufferSize = 32 * 1024

	DefaultDialTimeout      = time.Second
	DefaultDrainTimeout     = time.Second * 5
	DefaultFlushInterval    = time.Second
	DefaultMaxRetries       = 10
	DefaultProtocol         = "udp"
	DefaultReconnectBufSize = 64 * 1024 * 1024 // 64MB
	DefaultReconnectWait    = time.Second
	DefaultStatsdPort       = 8125
	defaultStatsdPortString = "8125"
	DefaultWriteTimeout     = time.Millisecond * 100
	DefaultURL              = "udp://127.0.0.1:8125"
	defaultWriteBufferSize  = 32 * 1024 // WARN: remove
	flushChanSize           = 1024
	maxReconnectAttempts    = 60
)

var (
	ErrConnectionClosed     = errors.New("stats: connection closed")
	ErrReconnectBufExceeded = errors.New("stats: reconnect buffer size exceeded")
	ErrInvalidTimeout       = errors.New("stats: invalid timeout duration")
	ErrTimeout              = errors.New("stats: timeout")
)

// TODO: remove if unused
//
// Option is a function on the options for a connection.
type Option func(*Options) error

// CustomDialer can be used to specify any dialer, not necessarily
// a *net.Dialer.
type CustomDialer interface {
	Dial(network, address string) (net.Conn, error)
}

// TODO (CEV): organize options
//
// Options can be used to create a customized connection.
type Options struct {
	// TODO: convert 'localhost' to '127.0.0.1' so that we don't
	// accidentally use IPv6 ???
	//
	// URL is the statsd server the client will connect and send stats to.
	// If empty DefaultURL is used.
	URL string

	// The communication protocol used to send messages ("upd", "tcp")
	// Protocol is the network protocol used to send messages.  It will
	// only be used if URL does specify a protocol.  The default is UDP.
	Protocol string

	// ReconnectWait sets the time to backoff after attempting a reconnect
	// to a server that we were already connected to previously.
	ReconnectWait time.Duration

	// DialTimeout sets the timeout for a Dial operation on a connection.
	DialTimeout time.Duration

	// DrainTimeout sets the timeout for a Drain Operation to complete.
	DrainTimeout time.Duration

	// WriteTimeout is the maximum time to wait for write operations to the
	// statsd server to complete.
	WriteTimeout time.Duration

	// TODO: add an AlwaysFlush option
	//
	// FlushInterval sets the interval at which buffered stats will be flushed
	// to the server.
	FlushInterval time.Duration

	// ReconnectBufSize is the size of the backing bufio during reconnect.
	// Once this has been exhausted publish operations will return an error.
	ReconnectBufSize int

	// TODO:
	//  * Add an example using a buffer, logger or null sink.
	//  * Note that URL can be omitted since the DefaultURL will be used.
	//
	// CustomDialer allows specifying a custom dialer.  This allows using a
	// dialer that is not necessarily a *net.Dialer.
	CustomDialer CustomDialer

	// WARN WARN WARN: don't use the standard logger
	// TODO: set a no-op logger
	Log *log.Logger // WARN

	// NoRandomizeHosts configures whether we randomize the order of hosts when
	// connecting.
	NoRandomizeHosts bool

	// WARN (CEV): handle UDP limitations and optimal MTU size
	//
	// WriteBufferSize configures the size of the write buffer
	WriteBufferSize int
}

func (o *Options) dial(u *url.URL) (net.Conn, error) {
	var hosts []string

	// Handle the case where the provided host is not an IP address
	if net.ParseIP(u.Hostname()) == nil {
		addrs, _ := net.LookupHost(u.Hostname()) // ignore error
		for _, addr := range addrs {
			hosts = append(hosts, net.JoinHostPort(addr, u.Port()))
		}
	}

	// Use the provided host.
	if len(hosts) == 0 {
		hosts = append(hosts, u.Host)
	}

	if len(hosts) > 1 && !o.NoRandomizeHosts {
		rand.Shuffle(len(hosts), func(i, j int) {
			hosts[i], hosts[j] = hosts[j], hosts[i]
		})
	}

	dialer := o.CustomDialer
	if dialer == nil {
		dialer = &net.Dialer{
			Timeout: o.DialTimeout / time.Duration(len(hosts)),
		}
	}

	var err error
	var conn net.Conn
	for _, host := range hosts {
		conn, err = dialer.Dial(u.Scheme, host)
		if err == nil {
			break
		}
	}
	return conn, err
}

//////////////////////////////////////////////////////////////////

// TODO: consider removing
func parseURL(rawurl, defaultProtocol string) (*url.URL, error) {
	if !strings.Contains(rawurl, "://") {
		rawurl = fmt.Sprintf("%s://%s", defaultProtocol, rawurl)
	}
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if u.Port() != "" {
		return u, nil
	}

	// try adding default port
	if !strings.HasSuffix(rawurl, ":") {
		rawurl += ":"
	}
	rawurl += defaultStatsdPortString
	u, err = url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if u.Port() != "" {
		return u, nil
	}

	return nil, fmt.Errorf("stats: setting default port for URL: %q", rawurl)
}

//////////////////////////////////////////////////////////////////

type Conn struct {
	queue []byte
	bw    *bufio.Writer
	w     io.Writer
}

func (c *Conn) Write(p []byte) (int, error) {
	return -1, nil
}

// TODO: add optional logger
// TODO: consider setting write deadline
type udpConn struct {
	opts    Options
	url     *url.URL
	bw      *bufio.Writer
	pending *bytes.Buffer
	conn    *net.UDPConn
	mu      sync.Mutex
	network string
	raddr   string
}

// TODO: maybe rename to 'dial'
func newUDPWriter(opts Options) (*udpConn, error) {
	url, err := parseURL(opts.URL, opts.Protocol)
	if err != nil {
		return nil, err
	}
	w := &udpConn{
		url:  url,
		opts: opts,
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

func (c *udpConn) setURL() (err error) {
	if c.url == nil {
		c.url, err = parseURL(c.opts.URL, c.opts.Protocol)
	}
	return err
}

func (c *udpConn) connect() error {
	if c.url == nil {
		if err := c.setURL(); err != nil {
			return err
		}
	}

	conn, err := c.opts.dial(c.url)
	if err != nil {
		return err
	}
	udp, ok := conn.(*net.UDPConn)
	if !ok {
		return fmt.Errorf("stats: invalid conn type for UDP address (%s): %T",
			c.url, conn)
	}
	c.conn = udp

	if c.pending != nil && c.bw != nil {
		c.bw.Flush() // flush to pending buffer
	}

	var w io.Writer = c.conn
	if c.opts.WriteTimeout > 0 {
		w = &timeoutWriter{
			timeout: c.opts.WriteTimeout,
			conn:    c.conn,
		}
	}
	c.bw = bufio.NewWriterSize(w, c.opts.WriteBufferSize)
	return nil
}

func (c *udpConn) Close() (err error) {
	c.mu.Lock()
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
	return
}

func (c *udpConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	n, err := c.writeAndRetry(b, 0)
	c.mu.Unlock()
	return n, err
}

func (c *udpConn) writeAndRetry(b []byte, retryCount int) (n int, err error) {
	if c.conn != nil {
		to := time.Now().Add(WriteTimeout)
		if err = c.conn.SetWriteDeadline(to); err != nil {
			return // WARN: what to do here?
		}
		if n, err = c.conn.Write(b); err == nil {
			// TODO: handle partial writes which
			// may occur if there is a timeout
			return
		}
		// prevent an infinite retry loop
		retryCount++
		// TODO: make this configurable
		if retryCount >= maxReconnectAttempts {
			return
		}
	}
	// TODO: backoff
	if err = c.connect(); err != nil {
		return
	}
	return c.writeAndRetry(b, retryCount)
}

//////////////////////////////////////////////////////////////////

type timeoutWriter struct {
	timeout time.Duration
	conn    net.Conn
	err     error
}

func (w *timeoutWriter) Write(b []byte) (int, error) {
	var n int
	if w.err == nil {
		// CEV: ignore deadline errors as there is nothing we
		// can do about them.
		w.conn.SetWriteDeadline(time.Now().Add(w.timeout))
		n, w.err = w.conn.Write(b)
		w.conn.SetWriteDeadline(time.Time{})
	}
	return n, w.err
}

//////////////////////////////////////////////////////////////////

type chunkedWriter struct {
	size int
	conn net.Conn
	buf  []byte
	err  error
}

func (w *chunkedWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.buf == nil {
		w.buf = make([]byte, w.size)
	}
	// can't use *bytes.Reader because it implements io.WriterTo()
	r := lineReader{s: p}
	var n int64
	n, w.err = io.CopyBuffer(w.conn, &r, w.buf)
	return int(n), w.err
}

//////////////////////////////////////////////////////////////////

// use lineReader to prevent stat lines from being split
type lineReader struct {
	s []byte
	i int // current reading index
}

func (r *lineReader) Read(b []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	// don't split stat lines
	p := r.s[r.i:]
	if len(p) > len(b) {
		p = p[:len(b)]
		if i := bytes.LastIndexByte(p, '\n'); i != -1 {
			p = p[:i+1]
		}
	}
	n := copy(b, p)
	r.i += n
	return n, nil
}
