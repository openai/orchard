package context

import (
	"github.com/cirruslabs/orchard/internal/config"
	"github.com/spf13/cobra"
)

func newDefaultCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "default NAME",
		Aliases: []string{"set", "use"},
		Short:   "Set a default context to use for client/worker commands",
		Args:    cobra.ExactArgs(1),
		RunE:    runDefault,
	}

	return command
}

func runDefault(cmd *cobra.Command, args []string) error {
	name := args[0]

	configHandle, err := config.NewHandle()
	if err != nil {
		return err
	}

	return configHandle.SetDefaultContext(name)
}
