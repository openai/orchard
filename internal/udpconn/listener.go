package udpconn

import (
	"bytes"
	"net"
	"net/netip"
	"sync"
	"time"
)

const (
	// Maximum peers waiting for Accept().
	maxPendingPeers = 128

	// Maximum queued datagrams per peer.
	maxPacketsPerPeer = 16

	// Read buffer size to avoid truncating datagrams.
	maxDatagramSize = 65535

	// Close peers after this long without a client packet or a successful reply.
	idleTimeout = time.Minute
)

type Listener struct {
	// Shared UDP socket
	socket *net.UDPConn

	// Write serialization to the shared UDP socket
	activeWriter *Conn
	writeSlot    chan struct{}

	// Peer routing and queued connections for Accept()
	pending chan *Conn
	peers   map[netip.AddrPort]*Conn

	// Listener shutdown and background goroutine completion
	done     chan struct{}
	closeErr error
	wg       sync.WaitGroup

	mtx sync.Mutex
}

func Listen(network, address string) (net.Listener, error) {
	// Resolve the listener address and bind the shared UDP socket
	addr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}

	socket, err := net.ListenUDP(network, addr)
	if err != nil {
		return nil, err
	}

	// Configure shared socket buffers and release it if the setup fails
	if err := TuneSocket(socket); err != nil {
		_ = socket.Close()
		return nil, err
	}

	// Initialize listener
	listener := &Listener{
		socket:    socket,
		writeSlot: make(chan struct{}, 1),
		pending:   make(chan *Conn, maxPendingPeers),
		peers:     make(map[netip.AddrPort]*Conn),
		done:      make(chan struct{}),
	}

	// Make one write slot available
	listener.writeSlot <- struct{}{}

	// Start packet reception and idle expiration
	listener.wg.Go(listener.receive)
	listener.wg.Go(listener.expire)

	return listener, nil
}

func (l *Listener) Accept() (net.Conn, error) {
	// Wait for the next peer or for the listener to close
	for {
		select {
		case <-l.done:
			return nil, l.closeErr
		case conn := <-l.pending:
			// Skip peers that closed while queued
			if conn.closed() {
				continue
			}

			return conn, nil
		}
	}
}

func (l *Listener) Close() error {
	l.closeWithError(net.ErrClosed)
	l.wg.Wait()

	return nil
}

func (l *Listener) Addr() net.Addr { return l.socket.LocalAddr() }

func (l *Listener) receive() {
	// Reuse one buffer for socket reads and copy packets when dispatching
	buffer := make([]byte, maxDatagramSize)

	for {
		// Read the next datagram and close the listener if reception fails
		n, address, err := l.socket.ReadFromUDPAddrPort(buffer)
		if err != nil {
			l.closeWithError(err)

			return
		}

		// Route the datagram to the peer associated with its source address
		l.dispatch(address, buffer[:n])
	}
}

func (l *Listener) dispatch(address netip.AddrPort, payload []byte) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	// Ignore packets if the listener is closing
	if l.closeErr != nil {
		return
	}

	// Find the peer or create one for a new source address
	conn := l.peers[address]
	if conn == nil {
		// Register the peer and make it available to Accept()
		conn = &Conn{
			listener: l,
			address:  address,
			packets:  make(chan []byte, maxPacketsPerPeer),
			done:     make(chan struct{}),
		}

		select {
		case l.pending <- conn:
			l.peers[address] = conn
		default:
			// Pending peer queue is full
			return
		}
	}

	// Refresh activity even if the peer's packet queue is full
	conn.lastActivity = time.Now()

	// Try to queue the datagram
	select {
	case conn.packets <- bytes.Clone(payload):
		// Datagram queued successfully
	default:
		// Peer's queue is full
	}
}

func (l *Listener) expire() {
	// Stop expiration when the listener closes
	for {
		select {
		case <-l.done:
			return
		case now := <-time.After(time.Second):
			l.expireIdle(now)
		}
	}
}

func (l *Listener) expireIdle(now time.Time) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	// Close inactive peers
	for _, conn := range l.peers {
		if now.Sub(conn.lastActivity) >= idleTimeout {
			conn.closeLocked()
		}
	}
}

func (l *Listener) closeWithError(err error) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	// Preserve the first error and close the listener only once
	if l.closeErr != nil {
		return
	}
	l.closeErr = err

	// Unblock Accept() and socket I/O before closing every peer
	close(l.done)
	_ = l.socket.Close()

	// Close every peer
	for _, conn := range l.peers {
		conn.closeLocked()
	}
}
