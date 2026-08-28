package proxy

import (
	"errors"
	"io"
	"net"
	"strings"
)

// halfCloseConnection can stop writing without interrupting concurrent reads.
type halfCloseConnection interface {
	net.Conn
	CloseWrite() error
}

// Connections copies bytes in both directions. When both connections support
// write-side shutdown, EOF is propagated independently in each direction.
// Other connection pairs retain the historical close-on-first-completion behavior.
func Connections(left io.ReadWriteCloser, right io.ReadWriteCloser) (finalErr error) {
	if left, ok := left.(halfCloseConnection); ok {
		if right, ok := right.(halfCloseConnection); ok {
			return connectionsWithHalfClose(left, right)
		}
	}

	leftErrCh := make(chan error, 1)
	rightErrCh := make(chan error, 1)

	recordErr := func(newErr error) {
		if newErr != nil && finalErr == nil {
			finalErr = newErr
		}
	}

	go func() {
		_, err := io.Copy(left, right)
		rightErrCh <- err
	}()

	go func() {
		_, err := io.Copy(right, left)
		leftErrCh <- err
	}()

	// Wait for some goroutine and then unlock the other goroutine
	// by closing its source io.Reader
	select {
	case err := <-rightErrCh:
		recordErr(err)
		recordErr(left.Close())
		recordErr(<-leftErrCh)
	case err := <-leftErrCh:
		recordErr(err)
		recordErr(right.Close())
		recordErr(<-rightErrCh)
	}

	if finalErr != nil && strings.Contains(finalErr.Error(), "use of closed network connection") {
		finalErr = nil
	}

	return finalErr
}

// connectionsWithHalfClose keeps the reverse direction open after a clean EOF.
func connectionsWithHalfClose(left, right halfCloseConnection) error {
	//nolint:mnd // one result from each copy direction
	results := make(chan error, 2)

	copyHalf := func(destination, source halfCloseConnection) {
		_, err := io.Copy(destination, source)
		if err == nil {
			err = destination.CloseWrite()
		}

		results <- err
	}

	closeBoth := func() {
		_ = left.Close()
		_ = right.Close()
	}

	meaningfulError := func(err error) error {
		if err == nil ||
			errors.Is(err, net.ErrClosed) ||
			strings.Contains(err.Error(), "use of closed network connection") {
			return nil
		}

		return err
	}

	go copyHalf(right, left)
	go copyHalf(left, right)

	firstErr := <-results
	if firstErr != nil {
		// An I/O or CloseWrite failure must unblock both copy operations.
		closeBoth()
	}

	secondErr := <-results
	closeBoth()

	if err := meaningfulError(firstErr); err != nil {
		return err
	}

	return meaningfulError(secondErr)
}
