package base_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/worker/vmmanager/base"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

type shellTestDialer struct {
	address   string
	connected chan net.Conn
}

func (dialer shellTestDialer) Dial(network, address string) (net.Conn, error) {
	return dialer.DialContext(context.Background(), network, address)
}

func (dialer shellTestDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, network, dialer.address)
	if err == nil {
		dialer.connected <- connection
	}
	return connection, err
}

func TestShellDrainsBothSSHStreams(t *testing.T) {
	for _, scenario := range []struct {
		name           string
		stdout, stderr int
		exit           uint32
	}{
		{name: "large stderr", stdout: 1, stderr: 4096},
		{name: "large stdout", stdout: 4096, stderr: 1},
		{name: "both streams", stdout: 4096, stderr: 4096},
		{name: "small streams", stdout: 1, stderr: 1},
		{name: "empty"},
		{name: "exit status", stdout: 1, stderr: 1, exit: 7},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			_, privateKey, err := ed25519.GenerateKey(rand.Reader)
			require.NoError(t, err)
			signer, err := ssh.NewSignerFromKey(privateKey)
			require.NoError(t, err)
			config := &ssh.ServerConfig{NoClientAuth: true}
			config.AddHostKey(signer)
			listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()
			serverResult := make(chan error, 1)
			go func() {
				serverResult <- serveShellOutput(listener, config, scenario.stdout, scenario.stderr, scenario.exit)
			}()
			dialer := shellTestDialer{address: listener.Addr().String(), connected: make(chan net.Conn, 1)}
			var outCount, errCount int
			var callbacks atomic.Int32
			var concurrent atomic.Bool
			consume := func(line string) {
				if callbacks.Add(1) != 1 {
					concurrent.Store(true)
				}
				defer callbacks.Add(-1)
				runtime.Gosched()
				if strings.HasPrefix(line, "stdout:") {
					outCount++
				}
				if strings.HasPrefix(line, "stderr:") {
					errCount++
				}
			}
			result := make(chan error, 1)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			go func() {
				result <- new(base.VM).Shell(ctx, "synthetic", "synthetic", "fixture\n", consume, dialer,
					func(context.Context) (string, error) { return "192.0.2.1", nil })
			}()
			var connection net.Conn
			select {
			case connection = <-dialer.connected:
			case <-ctx.Done():
				t.Fatal("SSH connection did not start")
			}
			defer connection.Close()
			select {
			case err = <-result:
			case <-time.After(2 * time.Second):
				_ = connection.Close()
				select {
				case <-result:
				case <-time.After(time.Second):
					t.Fatal("Shell did not release closed connection")
				}
				t.Fatal("SSH script stalled while stderr was waiting for stdout to close")
			}
			if scenario.exit == 0 {
				require.NoError(t, err)
			} else {
				var exitError *ssh.ExitError
				require.ErrorAs(t, err, &exitError)
				require.EqualValues(t, scenario.exit, exitError.ExitStatus())
			}
			require.NoError(t, <-serverResult)
			require.Equal(t, scenario.stdout, outCount)
			require.Equal(t, scenario.stderr, errCount)
			require.False(t, concurrent.Load(), "consumeLine must remain serialized")
		})
	}
}

func serveShellOutput(listener net.Listener, config *ssh.ServerConfig, stdout, stderr int, exit uint32) error {
	connection, err := listener.Accept()
	if err != nil {
		return err
	}
	defer connection.Close()
	server, channels, requests, err := ssh.NewServerConn(connection, config)
	if err != nil {
		return err
	}
	defer server.Close()
	go ssh.DiscardRequests(requests)
	channelRequest, ok := <-channels
	if !ok {
		return io.ErrUnexpectedEOF
	}
	channel, requests, err := channelRequest.Accept()
	if err != nil {
		return err
	}
	defer channel.Close()
	for request := range requests {
		if request.Type != "shell" {
			_ = request.Reply(false, nil)
			continue
		}
		if err := request.Reply(true, nil); err != nil {
			return err
		}
		input := make([]byte, len("fixture\n"))
		if _, err := io.ReadFull(channel, input); err != nil {
			return err
		}
		if string(input) != "fixture\n" {
			return errors.New("unexpected synthetic script")
		}
		stderrText := strings.Repeat("stderr:"+strings.Repeat("x", 1016)+"\n", stderr)
		if _, err := io.WriteString(channel.Stderr(), stderrText); err != nil {
			return err
		}
		if _, err := io.WriteString(channel, strings.Repeat("stdout:"+strings.Repeat("y", 1016)+"\n", stdout)); err != nil {
			return err
		}
		if _, err := channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{exit})); err != nil {
			return err
		}
		if err := channel.Close(); err != nil {
			return err
		}
		// Wait for the peer's channel-close acknowledgement before closing TCP.
		for range requests {
		}
		return nil
	}
	return io.ErrUnexpectedEOF
}
