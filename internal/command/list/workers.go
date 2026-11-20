package list

import (
	"fmt"
	"time"

	"github.com/cirruslabs/orchard/pkg/client"
	"github.com/dustin/go-humanize"
	"github.com/gosuri/uitable"
	"github.com/spf13/cobra"
)

func newListWorkersCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "workers",
		Short: "List workers",
		RunE:  runListWorkers,
	}

	return command
}

func runListWorkers(cmd *cobra.Command, args []string) error {
	client, err := client.New()
	if err != nil {
		return err
	}

	workers, err := client.Workers().List(cmd.Context())
	if err != nil {
		return err
	}

	if quiet {
		for _, worker := range workers {
			fmt.Println(worker.Name)
		}

		return nil
	}

	table := uitable.New()
	table.Wrap = true

	table.AddRow("Name", "Address", "Last seen", "Scheduling paused")

	for _, worker := range workers {
		lastSeenInfo := humanize.RelTime(worker.LastSeen, time.Now(), "ago", "in the future")

		table.AddRow(worker.Name, worker.Address, lastSeenInfo, worker.SchedulingPaused)
	}

	fmt.Println(table)

	return nil
}
