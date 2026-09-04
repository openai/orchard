//nolint:testpackage // exercise peer state and expiry directly
package udpconn

import (
	"net"
	"net/netip"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConnCloseKeepsOtherPeersAlive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		listener := newTestListener(t)
		first := newTestPeer(t, listener)
		second := newTestPeer(t, listener)

		// Block the first peer's read and hold the shared write slot
		<-listener.writeSlot
		var readErr, writeErr error
		go func() { _, readErr = first.Read(nil) }()
		go func() { _, writeErr = first.Write([]byte("first")) }()
		synctest.Wait()

		// Closing the peer must release both operations
		require.NoError(t, first.Close())
		synctest.Wait()
		require.ErrorIs(t, readErr, net.ErrClosed)
		require.ErrorIs(t, writeErr, net.ErrClosed)
		require.NotContains(t, listener.peers, first.address)
		require.Same(t, second, listener.peers[second.address])

		// The second peer can still use the shared socket
		listener.writeSlot <- struct{}{}
		_, err := second.Write([]byte("second"))
		require.NoError(t, err)
	})
}

func TestListenerCloseUnblocksIO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		listener := newTestListener(t)
		reader := newTestPeer(t, listener)
		writer := newTestPeer(t, listener)
		address := listener.Addr().String()

		// Wait until accepting, reading, and waiting for a write slot are blocked
		<-listener.writeSlot
		var acceptErr, readErr, writeErr error
		go func() { _, acceptErr = listener.Accept() }()
		go func() { _, readErr = reader.Read(nil) }()
		go func() { _, writeErr = writer.Write([]byte("reply")) }()
		synctest.Wait()

		// Closing the listener must release every operation
		require.NoError(t, listener.Close())
		synctest.Wait()
		require.ErrorIs(t, acceptErr, net.ErrClosed)
		require.ErrorIs(t, readErr, net.ErrClosed)
		require.ErrorIs(t, writeErr, net.ErrClosed)

		// Another listener can immediately reuse the port
		rebound, err := (&net.ListenConfig{}).ListenPacket(t.Context(), "udp4", address)
		require.NoError(t, err)
		require.NoError(t, rebound.Close())
	})
}

func TestListenerExpiresOnlyIdlePeers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		listener := newTestListener(t)
		idle := newTestPeer(t, listener)
		active := newTestPeer(t, listener)

		// Expire one idle peer while the other has recent activity
		now := time.Now()
		idle.lastActivity = now.Add(-idleTimeout)
		active.lastActivity = now
		listener.expireIdle(now)

		require.True(t, idle.closed())
		require.NotContains(t, listener.peers, idle.address)
		require.False(t, active.closed())
		require.Same(t, active, listener.peers[active.address])

		// A packet from the expired sender creates a fresh peer
		var replacement net.Conn
		var acceptErr error
		go func() { replacement, acceptErr = listener.Accept() }()
		synctest.Wait()

		const packet = "new session"
		listener.dispatch(idle.address, []byte(packet))
		synctest.Wait()
		require.NoError(t, acceptErr)
		require.NotSame(t, idle, replacement)

		buffer := make([]byte, len(packet))
		n, err := replacement.Read(buffer)
		require.NoError(t, err)
		require.Equal(t, packet, string(buffer[:n]))

		// Closing the old peer again must leave its replacement registered
		require.NoError(t, idle.Close())
		require.Same(t, replacement, listener.peers[idle.address])
	})
}

func TestTrafficResetsIdleTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		listener := newTestListener(t)
		receiving := newTestPeer(t, listener)
		replying := newTestPeer(t, listener)

		// Receive and reply halfway through the original idle timeout
		time.Sleep(idleTimeout / 2)
		listener.dispatch(receiving.address, []byte("request"))
		_, err := replying.Write([]byte("reply"))
		require.NoError(t, err)

		// Both peers survive their original expiry time
		time.Sleep(idleTimeout / 2)
		listener.expireIdle(time.Now())
		require.False(t, receiving.closed())
		require.False(t, replying.closed())

		// Both expire after a full idle timeout without further traffic
		time.Sleep(idleTimeout / 2)
		listener.expireIdle(time.Now())
		require.True(t, receiving.closed())
		require.True(t, replying.closed())
	})
}

func newTestListener(t *testing.T) *Listener {
	t.Helper()

	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)

	// Omit background loops so tests control packet dispatch and expiry
	listener := &Listener{
		socket:    socket,
		pending:   make(chan *Conn),
		peers:     make(map[netip.AddrPort]*Conn),
		done:      make(chan struct{}),
		writeSlot: make(chan struct{}, 1),
	}
	listener.writeSlot <- struct{}{}
	t.Cleanup(func() { _ = listener.Close() })

	return listener
}

func newTestPeer(t *testing.T, listener *Listener) *Conn {
	t.Helper()

	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = socket.Close() })

	address, err := netip.ParseAddrPort(socket.LocalAddr().String())
	require.NoError(t, err)

	peer := &Conn{
		listener:     listener,
		address:      address,
		packets:      make(chan []byte, maxPacketsPerPeer),
		done:         make(chan struct{}),
		lastActivity: time.Now(),
	}
	listener.peers[address] = peer

	return peer
}
