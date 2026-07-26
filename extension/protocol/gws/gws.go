// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package gws

import (
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/lxzan/gws"
)

var (
	// ErrClosed .
	ErrClosed = errors.New("websocket listener closed")
	// ErrInvalidMessage .
	ErrInvalidMessage = errors.New("invalid message")
	// ErrInvalidMessageType .
	ErrInvalidMessageType = errors.New("invalid message type")
)

// Listener .
type Listener struct {
	addr     net.Addr
	upgrader *gws.Upgrader
	chAccept chan net.Conn
	chClose  chan struct{}
	closed   uint32
}

// Handler .
func (ln *Listener) Handler(w http.ResponseWriter, r *http.Request) {
	c, err := ln.upgrader.Upgrade(w, r)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	wsc := &Conn{Conn: c, chHandler: make(chan func(), 1)}
	select {
	case ln.chAccept <- wsc:
	case <-ln.chClose:
		_ = c.WriteClose(100, nil)
	}
}

// Close .
func (ln *Listener) Close() error {
	if atomic.CompareAndSwapUint32(&ln.closed, 0, 1) {
		close(ln.chClose)
	}
	return nil
}

// Addr .
func (ln *Listener) Addr() net.Addr {
	return ln.addr
}

// Accept .
func (ln *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-ln.chAccept:
		return c, nil
	case <-ln.chClose:
	}
	return nil, ErrClosed
}

// Conn wraps gws.Conn to net.Conn
type Conn struct {
	*gws.Conn
	chHandler chan func()
	message   *gws.Message
	buffer    []byte
}

func (c *Conn) Close() error {
	if c.message != nil {
		_ = c.message.Close()
		c.message = nil
	}
	return c.WriteClose(1000, nil)
}

// HandleWebsocket .
func (c *Conn) HandleWebsocket(handler func()) {
	select {
	case c.chHandler <- handler:
	default:
	}
}

// Read .
func (c *Conn) Read(b []byte) (int, error) {
	if len(c.buffer) == 0 {
		message, err := c.ReadMessage()
		if err != nil {
			return 0, err
		}
		c.message = message
		c.buffer = message.Bytes()
	}

	cbl := len(c.buffer)
	if cbl <= len(b) {
		copy(b[:cbl], c.buffer)
		c.buffer = nil
		_ = c.message.Close()
		c.message = nil
		return cbl, nil
	}
	copy(b, c.buffer[:len(b)])
	c.buffer = c.buffer[len(b):]
	return len(b), nil
}

// Write .
func (c *Conn) Write(b []byte) (int, error) {
	err := c.WriteMessage(gws.OpcodeBinary, b)
	if err == nil {
		return len(b), nil
	}
	return 0, err
}

// SetDeadline .
func (c *Conn) SetDeadline(t time.Time) error {
	err := c.SetReadDeadline(t)
	if err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

// Listen wraps websocket listen
func Listen(addr string, option *gws.ServerOption) (net.Listener, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	ln := &Listener{
		addr:     tcpAddr,
		upgrader: gws.NewUpgrader(&gws.BuiltinEventHandler{}, option),
		chAccept: make(chan net.Conn, 4096),
		chClose:  make(chan struct{}),
	}
	return ln, nil
}

// Dial wraps websocket dial
//func Dial(url string, args ...interface{}) (net.Conn, error) {
//	dialer := websocket.DefaultDialer
//	if len(args) > 0 {
//		d, ok := args[0].(*websocket.Dialer)
//		if ok {
//			dialer = d
//		}
//	}
//	c, _, err := dialer.Dial(url, nil)
//	if err != nil {
//		return nil, err
//	}
//	return &Conn{Conn: c}, nil
//}
