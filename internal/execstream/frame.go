package execstream

import (
	"context"
	"encoding/json"

	"github.com/coder/websocket"
)

type FrameType string

const (
	FrameTypeStdin  FrameType = "stdin"
	FrameTypeResize FrameType = "resize"
	FrameTypeStdout FrameType = "stdout"
	FrameTypeStderr FrameType = "stderr"
	FrameTypeExit   FrameType = "exit"
	FrameTypeError  FrameType = "error"
)

type Frame struct {
	Type     FrameType     `json:"type"`
	Data     []byte        `json:"data,omitempty"`
	Terminal *TerminalSize `json:"terminal,omitempty"`
	Exit     *Exit         `json:"exit,omitempty"`
	Error    string        `json:"error,omitempty"`
}

type Exit struct {
	Code int32 `json:"code"`
}

type TerminalSize struct {
	Rows uint32 `json:"rows"`
	Cols uint32 `json:"cols"`
}

func WriteFrame(ctx context.Context, wsConn *websocket.Conn, frame *Frame) error {
	frameBytes, err := json.Marshal(frame)
	if err != nil {
		return err
	}

	return wsConn.Write(ctx, websocket.MessageText, frameBytes)
}
