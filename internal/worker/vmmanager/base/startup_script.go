package base

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"

	"github.com/avast/retry-go/v4"
	"github.com/cirruslabs/orchard/internal/worker/socketalias"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	guestagent "github.com/cirruslabs/tart-guest-agent/pkg/v1"
	"github.com/dustin/go-humanize"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (vm *VM) shellTartGuestAgent(ctx context.Context, script string, consumeLine func(string)) error {
	path, err := vm.onDiskName.ControlSocketPath()
	if err != nil {
		return err
	}

	conn, err := grpc.NewClient("passthrough:///tart-guest-agent",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return socketalias.DialContext(ctx, path)
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create Tart Guest Agent client: %w", err)
	}
	defer conn.Close()

	stream, err := retry.DoWithData(func() (guestagent.Agent_ExecClient, error) {
		return guestagent.NewAgentClient(conn).Exec(ctx)
	}, retry.Context(ctx), retry.OnRetry(func(n uint, err error) {
		consumeLine(fmt.Sprintf("attempt %d to open Tart Guest Agent execution stream failed: %v", n, err))
	}))
	if err != nil {
		return fmt.Errorf("failed to open Tart Guest Agent execution stream: %w", err)
	}

	// Start the shell process
	if err := stream.Send(&guestagent.ExecRequest{
		Type: &guestagent.ExecRequest_Command_{
			Command: &guestagent.ExecRequest_Command{
				Name:        lo.Ternary(vm.os == v1.OSDarwin, "/bin/zsh", "/bin/bash"),
				Args:        []string{"-l"},
				Interactive: true,
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to send Tart Guest Agent command: %w", err)
	}

	// Feed it our startup script
	if err := stream.Send(&guestagent.ExecRequest{
		Type: &guestagent.ExecRequest_StandardInput{
			StandardInput: &guestagent.IOChunk{
				Data: []byte(script),
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to send Tart Guest Agent script: %w", err)
	}

	if err := stream.CloseSend(); err != nil {
		return err
	}

	// Wait for the shell process to finish,
	// retrieving its outputs and exit code
	stdout := newScriptOutput(consumeLine)
	stderr := newScriptOutput(consumeLine)
	defer func() {
		stdout.flush()
		stderr.flush()
	}()

	for {
		response, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive Tart Guest Agent output: %w", err)
		}

		switch result := response.GetType().(type) {
		case *guestagent.ExecResponse_StandardOutput:
			stdout.write(result.StandardOutput.GetData())
		case *guestagent.ExecResponse_StandardError:
			stderr.write(result.StandardError.GetData())
		case *guestagent.ExecResponse_Exit_:
			if result.Exit.GetCode() != 0 {
				return fmt.Errorf("process exited with status %d", result.Exit.GetCode())
			}

			return nil
		}
	}
}

const (
	scriptOutputBufferSize = 128 * humanize.KiByte
)

type scriptOutput struct {
	consumeLine func(string)
	pending     *bytes.Buffer
	stopped     bool
}

func newScriptOutput(consumeLine func(string)) *scriptOutput {
	return &scriptOutput{
		consumeLine: consumeLine,
		pending:     bytes.NewBuffer(make([]byte, 0, scriptOutputBufferSize)),
	}
}

func (output *scriptOutput) write(data []byte) {
	if output.stopped {
		return
	}

	for len(data) > 0 {
		// Determine if we have a full line available in the incoming data
		advance, _, _ := bufio.ScanLines(data, false)

		if advance == 0 {
			// No newline yet; buffer the remainder
			output.pending.Write(data)

			break
		}

		// Complete the pending line and emit it
		output.pending.Write(data[:advance])
		output.flush()

		// Update slice to point to the next data chunk
		data = data[advance:]
	}

	output.stopped = output.pending.Len() >= scriptOutputBufferSize
}

func (output *scriptOutput) flush() {
	defer output.pending.Reset()

	if output.stopped {
		return
	}

	_, line, _ := bufio.ScanLines(output.pending.Bytes(), true)
	if line != nil {
		output.consumeLine(string(line))
	}
}
