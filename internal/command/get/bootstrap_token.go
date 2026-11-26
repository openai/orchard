package get

import (
	"fmt"
	"github.com/cirruslabs/orchard/internal/bootstraptoken"
	"github.com/cirruslabs/orchard/internal/config"
	"github.com/cirruslabs/orchard/pkg/client"
	"github.com/spf13/cobra"
)

func newGetBootstrapTokenCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "bootstrap-token NAME",
		Short: "Retrieve a bootstrap token for the specified service account",
		Long: "Retrieve a complete bootstrap token for the specified service account. " +
			"For worker registration, use a service account configured with worker permissions, " +
			"not the controller's bootstrap administrator. Pass the returned token to orchard worker run.",
		RunE: runGetBootstrapToken,
		Args: cobra.ExactArgs(1),
	}

	return command
}

func runGetBootstrapToken(cmd *cobra.Command, args []string) error {
	name := args[0]

	configHandle, err := config.NewHandle()
	if err != nil {
		return err
	}

	defaultContext, err := configHandle.DefaultContext()
	if err != nil {
		return err
	}

	client, err := client.New()
	if err != nil {
		return err
	}

	serviceAccount, err := client.ServiceAccounts().Get(cmd.Context(), name)
	if err != nil {
		return err
	}

	bootstrapToken, err := bootstraptoken.New(defaultContext.Certificate, serviceAccount.Name, serviceAccount.Token)
	if err != nil {
		return err
	}

	fmt.Println(bootstrapToken)

	return nil
}
