package get

import (
	"fmt"
	"strings"
	"time"

	"github.com/cirruslabs/orchard/internal/structpath"
	"github.com/cirruslabs/orchard/pkg/client"
	"github.com/dustin/go-humanize"
	"github.com/gosuri/uitable"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
)

func newGetWorkerCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "worker NAME",
		Short: "Retrieve a worker and it's fields",
		RunE:  runGetWorker,
		Args:  cobra.ExactArgs(1),
	}

	return command
}

func runGetWorker(cmd *cobra.Command, args []string) error {
	name := args[0]

	client, err := client.New()
	if err != nil {
		return err
	}

	// Ability to retrieve resource fields (e.g. "orchard get worker dev-mini/name")
	splits := strings.Split(name, "/")
	var path []string
	if len(splits) > 1 {
		name = splits[0]
		path = splits[1:]
	}

	worker, err := client.Workers().Get(cmd.Context(), name)
	if err != nil {
		return err
	}

	// Ability to retrieve resource fields (e.g. "orchard get worker dev-mini/name")
	if len(path) != 0 {
		result, ok := structpath.Lookup(*worker, path)
		if !ok {
			return fmt.Errorf("%w: failed to find the specified field \"%s\" or the field is not a string",
				ErrGetFailed, strings.Join(path, "/"))
		}

		fmt.Println(result)

		return nil
	}

	table := uitable.New()
	table.Wrap = true

	table.AddRow("Name", worker.Name)

	createdAtInfo := humanize.RelTime(worker.CreatedAt, time.Now(), "ago", "in the future")
	table.AddRow("Created", createdAtInfo)

	lastSeenInfo := humanize.RelTime(worker.LastSeen, time.Now(), "ago", "in the future")
	table.AddRow("Last seen", lastSeenInfo)

	table.AddRow("Machine ID", worker.MachineID)
	table.AddRow("Address", nonEmptyOrNone(worker.Address))
	table.AddRow("Scheduling paused", worker.SchedulingPaused)
	table.AddRow("Default CPU", worker.DefaultCPU)
	table.AddRow("Default memory", worker.DefaultCPU)

	resourcesInfo := strings.Join(lo.MapToSlice(worker.Resources, func(key string, value uint64) string {
		return fmt.Sprintf("%s: %d", key, value)
	}), "\n")
	table.AddRow("Resources", nonEmptyOrNone(resourcesInfo))

	labelsInfo := strings.Join(lo.MapToSlice(worker.Labels, func(key string, value string) string {
		return fmt.Sprintf("%s: %s", key, value)
	}), "\n")
	table.AddRow("Labels", nonEmptyOrNone(labelsInfo))

	fmt.Println(table)

	return nil
}
