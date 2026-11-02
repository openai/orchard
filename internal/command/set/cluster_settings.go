package set

import (
	"errors"
	"fmt"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/spf13/cobra"
)

var ErrClusterSettingsFailed = errors.New("failed to set cluster settings")

var hostDirPoliciesRaw []string
var schedulerProfileRaw string

const (
	hostDirPoliciesFlag  = "host-dir-policies"
	schedulerProfileFlag = "scheduler-profile"
)

func newSetClusterSettingsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster-settings",
		Short: "Set cluster settings",
		RunE:  runSetClusterSettings,
	}

	cmd.Flags().StringSliceVar(&hostDirPoliciesRaw, hostDirPoliciesFlag, []string{},
		fmt.Sprintf("comma-separated list of hostDir policies containing an allowed path prefix "+
			"and an optional \":ro\" modifier to only allow read-only mounts for that path prefix "+
			"(for example, --%s=/Users/ci/sources:ro,/tmp)", hostDirPoliciesFlag))
	cmd.Flags().StringVar(&schedulerProfileRaw, schedulerProfileFlag, "", fmt.Sprintf(
		`scheduler profile to use:

* --%s=%s — when scheduling a pending VM to a worker, pick the busiest worker that can fit a VM first,
falling back to less busier workers (this is the default behavior when no explicit scheduler profile is set)

* --%s=%s — when scheduling a pending VM to a worker, pick the least occupied worker that can fit a VM first,
falling back to more busier workers

`, schedulerProfileFlag, v1.SchedulerProfileOptimizeUtilization, schedulerProfileFlag,
		v1.SchedulerProfileDistributeLoad))

	return cmd
}

func runSetClusterSettings(cmd *cobra.Command, args []string) error {
	// Update cluster settings
	client, err := client.New()
	if err != nil {
		return err
	}

	var needUpdate bool

	clusterSettings, err := client.ClusterSettings().Get(cmd.Context())
	if err != nil {
		return err
	}

	if cmd.Flag(hostDirPoliciesFlag).Changed {
		for _, hostDirPolicyRaw := range hostDirPoliciesRaw {
			hostDirPolicy, err := v1.NewHostDirPolicyFromString(hostDirPolicyRaw)
			if err != nil {
				return err
			}

			clusterSettings.HostDirPolicies = append(clusterSettings.HostDirPolicies, hostDirPolicy)
		}

		needUpdate = true
	}

	if cmd.Flag(schedulerProfileFlag).Changed {
		clusterSettings.SchedulerProfile, err = v1.NewSchedulerProfile(schedulerProfileRaw)
		if err != nil {
			return err
		}

		needUpdate = true
	}

	// Check if we need to update anything in the cluster settings
	if !needUpdate {
		return fmt.Errorf("%w: you need to specify at least one setting to update", ErrClusterSettingsFailed)
	}

	return client.ClusterSettings().Set(cmd.Context(), clusterSettings)
}
