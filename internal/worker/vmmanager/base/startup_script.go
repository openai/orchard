package base

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/cirruslabs/orchard/internal/worker/socketalias"
	guestagent "github.com/cirruslabs/tart-guest-agent/pkg/v1"
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

	stream, err := guestagent.NewAgentClient(conn).Exec(ctx, grpc.WaitForReady(true))
	if err != nil {
		return fmt.Errorf("failed to open Tart Guest Agent execution stream: %w", err)
	}

	// Start the shell process
	if err := stream.Send(&guestagent.ExecRequest{
		Type: &guestagent.ExecRequest_Command_{
			Command: &guestagent.ExecRequest_Command{
				Name:        "/bin/zsh",
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
	stdout := scriptOutput{consumeLine: consumeLine}
	stderr := scriptOutput{consumeLine: consumeLine}
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

// Buffer partial lines across output chunks
type scriptOutput struct {
	consumeLine func(string)
	pending     []byte
}

func (output *scriptOutput) write(data []byte) {
	output.pending = append(output.pending, data...)

	for {
		line, rest, found := bytes.Cut(output.pending, []byte{'\n'})
		if !found {
			return
		}

		output.consumeLine(strings.TrimSuffix(string(line), "\r"))
		output.pending = rest
	}
}

func (output *scriptOutput) flush() {
	if len(output.pending) != 0 {
		output.consumeLine(strings.TrimSuffix(string(output.pending), "\r"))
		output.pending = nil
	}
}
