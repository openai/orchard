package udpconn

import (
	"errors"
	"io"
	"net"
	"net/netip"
	"time"
)

const (
	// Limit how long a socket write can block.
	writeTimeout = time.Second
)

type Conn struct {
	// Listener owning the shared socket
	listener *Listener

	// Our peer's remote address
	address netip.AddrPort

	// Queued datagrams and shutdown notification
	packets chan []byte
	done    chan struct{}

	// Time of the last received packet or successful write
	lastActivity time.Time
}

func (c *Conn) Read(buffer []byte) (int, error) {
	// Wait for a datagram or for the peer to close
	select {
	case <-c.done:
		return 0, net.ErrClosed
	case payload := <-c.packets:
		// Check whether the peer closed while waiting
		if c.closed() {
			return 0, net.ErrClosed
		}

		return copy(buffer, payload), nil
	}
}

func (c *Conn) Write(payload []byte) (int, error) {
	// Serialize writes because all peers share the socket's write deadline
	select {
	case <-c.done:
		return 0, net.ErrClosed
	case <-c.listener.writeSlot:
	}
	defer func() { c.listener.writeSlot <- struct{}{} }()

	// Set the write deadline and register the active writer while holding the state lock
	c.listener.mtx.Lock()
	if c.closed() {
		c.listener.mtx.Unlock()
		return 0, net.ErrClosed
	}
	if err := c.listener.socket.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		c.listener.mtx.Unlock()
		return 0, err
	}
	c.listener.activeWriter = c
	c.listener.mtx.Unlock()

	// Send without holding the state lock so Close can interrupt the write
	n, err := c.listener.socket.WriteToUDPAddrPort(payload, c.address)

	// Clear the active writer and record successful activity unless the peer closed
	c.listener.mtx.Lock()
	c.listener.activeWriter = nil
	if c.closed() {
		err = net.ErrClosed
	} else if err == nil {
		c.lastActivity = time.Now()
	}
	c.listener.mtx.Unlock()

	return n, err
}

func (c *Conn) Close() error {
	c.listener.mtx.Lock()
	defer c.listener.mtx.Unlock()

	c.closeLocked()

	return nil
}

func (c *Conn) LocalAddr() net.Addr  { return c.listener.Addr() }
func (c *Conn) RemoteAddr() net.Addr { return net.UDPAddrFromAddrPort(c.address) }

func (*Conn) SetDeadline(time.Time) error      { return errors.ErrUnsupported }
func (*Conn) SetReadDeadline(time.Time) error  { return errors.ErrUnsupported }
func (*Conn) SetWriteDeadline(time.Time) error { return errors.ErrUnsupported }

func (c *Conn) WriteTo(destination io.Writer) (int64, error) {
	// Allocate enough space to read a complete datagram
	buffer := make([]byte, maxDatagramSize)
	var total int64

	for {
		// Read the next datagram, including empty datagrams
		n, err := c.Read(buffer)
		if err != nil {
			return total, err
		}

		// Set a write timeout when the destination implements net.Conn
		if conn, ok := destination.(net.Conn); ok {
			if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				return total, err
			}
		}

		// Forward the datagram in one write and reject partial writes
		written, err := destination.Write(buffer[:n])
		total += int64(written)
		if err != nil {
			return total, err
		}
		if written != n {
			return total, io.ErrShortWrite
		}
	}
}

func (c *Conn) ReadFrom(source io.Reader) (int64, error) {
	// Allocate enough space to read a complete datagram
	buffer := make([]byte, maxDatagramSize)
	var total int64

	for {
		// Preserve empty datagrams and data returned alongside a read error
		n, readErr := source.Read(buffer)
		if n > 0 || readErr == nil {
			// Forward the datagram in one write and reject partial writes
			written, err := c.Write(buffer[:n])
			total += int64(written)
			if err != nil {
				return total, err
			}
			if written != n {
				return total, io.ErrShortWrite
			}
		}

		// Handle the source error after forwarding any accompanying data
		if readErr != nil {
			if readErr == io.EOF {
				return total, nil
			}
			return total, readErr
		}
	}
}

func (c *Conn) closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *Conn) closeLocked() {
	// Ignore repeated close requests
	if c.closed() {
		return
	}

	// Unblock waiting operations and remove the peer from routing
	close(c.done)
	delete(c.listener.peers, c.address)

	// Interrupt this peer's active write without closing the shared socket
	if c.listener.activeWriter == c {
		_ = c.listener.socket.SetWriteDeadline(time.Now())
	}
}
