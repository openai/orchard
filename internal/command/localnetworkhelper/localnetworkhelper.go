//go:build unix

package localnetworkhelper

import (
	"github.com/cirruslabs/orchard/internal/localnetworkhelper"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:    localnetworkhelper.CommandName,
		Short:  "Run the macOS \"Local Network\" permission helper process",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			// In localnetworkhelper.New(), a macOS "Local Network"
			// permission helper process is spawned and receives
			// its socketpair(2) end via ExtraFiles field of Golang's
			// exec.Cmd[1].
			//
			// This socketpair(2) end becomes FD number 3 here,
			// according to the ExtraFiles documentation[1]:
			//
			// >If non-nil, entry i becomes file descriptor 3+i.
			//
			// [1]: https://pkg.go.dev/os/exec#Cmd
			return localnetworkhelper.Serve(3)
		},
	}
	return cmd
}
