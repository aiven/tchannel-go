/*
 * Author: Markus Stenberg <mstenber@aiven.io>
 *
 * Copyright (c) 2019 Aiven Oy.
 *
 * Created:       Wed Nov 20 15:34:09 2019 mstenber
 * Last modified: Thu Nov 21 15:47:05 2019 mstenber
 * Edit time:     107 min
 *
 */

package tchannel

import (
	"github.com/pierrec/lz4"
	"net"
	"sync"
	"time"
)

type writeResponse struct {
	n   int
	err error
}

type closeResponse struct {
	err error
}

type writeRequest struct {
	b          []byte
	resultChan chan<- writeResponse
}

type closeRequest struct {
	resultChan chan<- closeResponse
}

// Transparent compression on top of existing conn; both reads and
// writes are automagically lz4'd. Writes have flush interval after
// which even partial output is written, and idle interval which
// causes flush if nothing new has been received during it. For now,
// the intervals are hardcoded.

type lz4Conn struct {
	conn          net.Conn            // original connection
	reader        *lz4.Reader         // pointed at .conn
	writer        *lz4.Writer         // pointed at .conn
	readMutex     sync.Mutex          // threadsafe reading
	writeChannel  chan<- writeRequest // threadsafe writing
	closeChannel  chan<- closeRequest // threadsafe close
	flushInterval time.Duration       // first write => flush
	idleInterval  time.Duration       // last write => flush
	closed        bool
}

func (self *lz4Conn) Read(b []byte) (n int, err error) {
	self.readMutex.Lock()
	defer self.readMutex.Unlock()
	return self.reader.Read(b)
}

func (self *lz4Conn) Write(b []byte) (n int, err error) {
	rc := make(chan writeResponse)
	req := writeRequest{b, rc}
	self.writeChannel <- req
	response := <-rc
	return response.n, response.err
}

func (self *lz4Conn) LocalAddr() net.Addr {
	return self.conn.LocalAddr()
}

func (self *lz4Conn) RemoteAddr() net.Addr {
	return self.conn.RemoteAddr()
}

func (self *lz4Conn) SetDeadline(t time.Time) error {
	return self.conn.SetDeadline(t)
}

func (self *lz4Conn) SetReadDeadline(t time.Time) error {
	return self.conn.SetReadDeadline(t)
}

func (self *lz4Conn) SetWriteDeadline(t time.Time) error {
	return self.conn.SetWriteDeadline(t)
}

func (self *lz4Conn) Close() error {
	if self.closed {
		return nil
	}
	self.closed = true
	rc := make(chan closeResponse)
	req := closeRequest{rc}
	self.closeChannel <- req
	response := <-rc
	if response.err != nil {
		return response.err
	}
	return self.conn.Close()
}

func (self *lz4Conn) worker(wc <-chan writeRequest, cc <-chan closeRequest) {
	it := time.NewTimer(self.idleInterval)
	ft := time.NewTimer(self.flushInterval)
	ftPending := false
	writes := 0
	for {
		select {
		case <-ft.C:
			ftPending = false
			if writes > 0 {
				self.writer.Flush()
			}
		case <-it.C:
			if writes > 0 {
				self.writer.Flush()
			}
		case req := <-wc:
			writes += 1
			n, err := self.writer.Write(req.b)
			req.resultChan <- writeResponse{n, err}
			// reset idle timer
			if !it.Stop() {
				select {
				case <-it.C:
				default:
				}
			}
			it.Reset(self.idleInterval)
			// start flush timer if necessary
			if !ftPending {
				ft.Reset(self.flushInterval)
				ftPending = true
			}
		case req := <-cc:
			// err := self.writer.Close()
			var err error = nil
			req.resultChan <- closeResponse{err}
			return
		}
	}
}

func NewLZ4Conn(conn net.Conn) net.Conn {
	_, ok := conn.(*lz4Conn)
	if ok {
		return conn
	}
	wc := make(chan writeRequest)
	cc := make(chan closeRequest)
	c := &lz4Conn{conn: conn,
		reader:        lz4.NewReader(conn),
		writer:        lz4.NewWriter(conn),
		flushInterval: 30 * time.Millisecond,
		idleInterval:  5 * time.Millisecond,
		writeChannel:  wc,
		closeChannel:  cc,
	}
	go c.worker(wc, cc)
	return c
}
