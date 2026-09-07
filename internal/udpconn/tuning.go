package udpconn

import (
	"net"

	"github.com/dustin/go-humanize"
)

const socketBuffer = humanize.MiByte

func TuneSocket(socket *net.UDPConn) error {
	if err := socket.SetReadBuffer(socketBuffer); err != nil {
		return err
	}

	return socket.SetWriteBuffer(socketBuffer)
}
