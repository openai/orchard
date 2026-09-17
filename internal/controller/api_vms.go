package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cirruslabs/orchard/internal/controller/lifecycle"
	"github.com/cirruslabs/orchard/internal/controller/scheduler"
	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/responder"
	"github.com/cirruslabs/orchard/internal/simplename"
	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/pkg/client"
	"github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/cirruslabs/orchard/rpc"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/samber/lo"
)

func (controller *Controller) createVM(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleComputeWrite); responder != nil {
		return responder
	}

	var vm v1.VM

	if err := ctx.ShouldBindJSON(&vm); err != nil {
		return responder.JSON(http.StatusBadRequest, NewErrorResponse("invalid JSON was provided"))
	}

	// Host processes require an additional role
	if len(vm.HostProcesses) != 0 {
		if responder := controller.authorize(ctx, v1.ServiceAccountRoleHostProcessWrite); responder != nil {
			return responder
		}
	}

	if vm.Name == "" {
		return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("VM name is empty"))
	} else if err := simplename.Validate(vm.Name); err != nil {
		return responder.JSON(http.StatusPreconditionFailed,
			NewErrorResponse("VM name %v", err))
	}
	if vm.Image == "" {
		return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("VM image is empty"))
	}
	if err := v1.ValidateEndpoints(vm.Endpoints); err != nil {
		return responder.JSON(http.StatusBadRequest, NewErrorResponse("invalid endpoints: %v", err))
	}

	// Provide defaults
	vm.Status = v1.VMStatusPending
	vm.CreatedAt = time.Now()
	vm.RestartedAt = time.Time{}
	vm.RestartCount = 0
	vm.UID = uuid.New().String()
	vm.PowerState = v1.PowerStateRunning
	vm.LocalName = ondiskname.New(vm.Name, vm.UID, vm.RestartCount).String()
	//nolint:staticcheck // yes, this is deprecated, but we still maintain it for backward compatibility
	vm.TartName = vm.LocalName
	vm.Generation = 0
	vm.ObservedGeneration = 0
	vm.ObservedEndpoints = nil
	vm.Conditions = []v1.Condition{
		{
			Type:  v1.ConditionTypeScheduled,
			State: v1.ConditionStateFalse,
		},
	}

	// Provide platform defaults
	if vm.OS == "" {
		vm.OS = v1.OSDarwin
	}
	if vm.Arch == "" {
		vm.Arch = v1.ArchitectureARM64
	}
	if vm.Runtime == "" {
		vm.Runtime = v1.RuntimeTart
	}

	if err := vm.Validate(); err != nil {
		return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("%v", err))
	}

	// Softnet-specific logic: automatically enable Softnet when NetSoftnetAllow, NetSoftnetBlock
	// or NetSoftnetExpose are set and propagate deprecated and non-deprecated boolean fields into each other
	if vm.NetSoftnetDeprecated || vm.NetSoftnet || len(vm.NetSoftnetAllow) != 0 || len(vm.NetSoftnetBlock) != 0 ||
		len(vm.NetSoftnetExpose) != 0 {
		vm.NetSoftnetDeprecated = true
		vm.NetSoftnet = true
	}

	// Apple limits the number of macOS VMs to 2,
	// so we need to provide a resource default
	// (if not otherwise overridden by the user)
	// to avoid a case when more than 2 VMs run
	if vm.OS == v1.OSDarwin && vm.Runtime == v1.RuntimeTart {
		if vm.Resources == nil {
			vm.Resources = make(v1.Resources)
		}
		if _, ok := vm.Resources[v1.ResourceTartVMs]; !ok {
			vm.Resources[v1.ResourceTartVMs] = 1
		}
	}

	// Validate image pull policy and provide a default value if it's missing
	if vm.ImagePullPolicy != "" {
		if _, err := v1.NewImagePullPolicyFromString(string(vm.ImagePullPolicy)); err != nil {
			return responder.JSON(http.StatusPreconditionFailed,
				NewErrorResponse("unsupported image pull policy: %q", vm.ImagePullPolicy))
		}
	} else {
		vm.ImagePullPolicy = v1.ImagePullPolicyIfNotPresent
	}

	// Validate restart policy and provide a default value if it's missing
	if vm.RestartPolicy != "" {
		if _, err := v1.NewRestartPolicyFromString(string(vm.RestartPolicy)); err != nil {
			return responder.JSON(http.StatusPreconditionFailed,
				NewErrorResponse("unsupported restart policy: %q", vm.RestartPolicy))
		}
	} else {
		vm.RestartPolicy = v1.RestartPolicyNever
	}

	// Validate hostProcesses
	if err := v1.ValidateHostProcesses(vm.HostProcesses); err != nil {
		return responder.JSON(http.StatusBadRequest, NewErrorResponse("invalid host processes: %v", err))
	}

	// Validate hostDirs
	if responder := controller.validateHostDirs(vm.HostDirs); responder != nil {
		return responder
	}

	response := controller.storeUpdate(func(txn storepkg.Transaction) responder.Responder {
		// Does the VM resource with this name already exists?
		_, err := txn.GetVM(vm.Name)
		if err != nil && !errors.Is(err, storepkg.ErrNotFound) {
			controller.logger.Errorf("failed to check if the VM exists in the DB: %v", err)

			return responder.Code(http.StatusInternalServerError)
		}
		if err == nil {
			return responder.JSON(http.StatusConflict, NewErrorResponse("VM with this name already exists"))
		}

		if err := txn.SetVM(vm); err != nil {
			controller.logger.Errorf("failed to create VM in the DB: %v", err)

			return responder.Code(http.StatusInternalServerError)
		}

		return responder.JSON(http.StatusOK, &vm)
	})
	// request immediate scheduling
	controller.scheduler.RequestScheduling()
	return response
}

func (controller *Controller) updateVMSpec(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleComputeWrite); responder != nil {
		return responder
	}

	var userVM v1.VM

	if err := ctx.ShouldBindJSON(&userVM); err != nil {
		return responder.JSON(http.StatusBadRequest, NewErrorResponse("invalid JSON was provided"))
	}
	if err := v1.ValidateEndpoints(userVM.Endpoints); err != nil {
		return responder.JSON(http.StatusBadRequest, NewErrorResponse("invalid endpoints: %v", err))
	}

	if responder := controller.validateHostDirs(userVM.HostDirs); responder != nil {
		return responder
	}

	name := ctx.Param("name")
	var affectedWorker string

	response := controller.storeUpdate(func(txn storepkg.Transaction) responder.Responder {
		// Re-initialize to guard against potential transaction retry
		affectedWorker = ""

		dbVM, err := txn.GetVM(name)
		if err != nil {
			return responder.Error(err)
		}

		// Changes to host processes require an additional role
		//nolint:staticcheck // Preserve the original explicit VMSpec comparison.
		if !dbVM.VMSpec.HostProcessesEqual(userVM.VMSpec) {
			if responder := controller.authorize(ctx, v1.ServiceAccountRoleHostProcessWrite); responder != nil {
				return responder
			}
		}

		if dbVM.TerminalState() {
			return responder.JSON(http.StatusPreconditionFailed,
				NewErrorResponse("cannot update VM in a terminal state"))
		}

		// Platform sanity checks
		if dbVM.OS != userVM.OS || dbVM.Arch != userVM.Arch || dbVM.Runtime != userVM.Runtime {
			return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("\"os\", \"arch\" "+
				"and \"runtime\" fields cannot be modified"))
		}

		if err := userVM.Validate(); err != nil {
			return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("%v", err))
		}

		// Validate hostProcesses
		if err := v1.ValidateHostProcesses(userVM.HostProcesses); err != nil {
			return responder.JSON(http.StatusBadRequest, NewErrorResponse("invalid host processes: %v", err))
		}

		// Softnet-specific logic: automatically enable Softnet when NetSoftnetAllow, NetSoftnetBlock
		// or NetSoftnetExpose are set and propagate deprecated and non-deprecated boolean fields into each other
		if userVM.NetSoftnetDeprecated || userVM.NetSoftnet || len(userVM.NetSoftnetAllow) != 0 ||
			len(userVM.NetSoftnetBlock) != 0 || len(userVM.NetSoftnetExpose) != 0 {
			userVM.NetSoftnetDeprecated = true
			userVM.NetSoftnet = true
		}

		// Suspendable-specific sanity checks
		if dbVM.Suspendable && !userVM.Suspendable {
			return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("\"suspendable\" cannot be "+
				"toggled for suspendable VMs"))
		}
		if dbVM.Suspendable && dbVM.NetSoftnet != userVM.NetSoftnet {
			return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("\"netSoftnet\" cannot be "+
				"toggled for suspendable VMs"))
		}

		// Power state-specific sanity checks
		if !userVM.PowerState.Valid() {
			return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("invalid \"powerState\" "+
				"value: %s", userVM.PowerState))
		}
		if dbVM.PowerState.TerminalState() {
			return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("invalid \"powerState\" "+
				"transition: cannot transition from a terminal power state"))
		}
		if !dbVM.Suspendable && userVM.PowerState == v1.PowerStateSuspended {
			return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("invalid \"powerState\" "+
				"transition: only suspendable VMs can be suspended"))
		}

		if v1.SemanticallyEqual(dbVM.VMSpec, userVM.VMSpec) {
			// Nothing was changed
			return responder.JSON(http.StatusOK, dbVM)
		}

		// A worker keeps the old ports bound until it stops the VM to apply the new
		// generation, so releasing them in the controller's view straight away would
		// let another VM be scheduled onto a port that is still in use
		if dbVM.IsScheduled() && !slices.Equal(dbVM.NetSoftnetExpose, userVM.NetSoftnetExpose) {
			return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("\"netSoftnetExpose\" "+
				"cannot be changed once the VM is scheduled on worker %q", dbVM.Worker))
		}

		// Endpoints can be changed on a VM that is already scheduled, which never goes
		// past the scheduler, so the worker it sits on is checked here instead
		if dbVM.IsScheduled() && !v1.SemanticallyEqual(dbVM.Endpoints, userVM.Endpoints) {
			vms, err := txn.ListVMs()
			if err != nil {
				controller.logger.Errorf("failed to list VMs in the DB: %v", err)

				return responder.Code(http.StatusInternalServerError)
			}

			updatedVM := *dbVM
			updatedVM.VMSpec = userVM.VMSpec

			// the whole specification has to come out clean: a VM being placed is not
			// refused a worker for a clash it did not make, but this one owns its own
			if scheduler.WorkerPortConflict(vms, dbVM.Worker, dbVM.Name, updatedVM) {
				return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse(
					"\"endpoints\" cannot take a worker port that another VM on worker %q holds",
					dbVM.Worker))
			}
		}

		// VM specification was changed
		dbVM.VMSpec = userVM.VMSpec
		dbVM.Generation++

		if err := txn.SetVM(*dbVM); err != nil {
			controller.logger.Errorf("failed to update VM in the DB: %v", err)

			return responder.Code(http.StatusInternalServerError)
		}

		affectedWorker = dbVM.Worker

		return responder.JSON(http.StatusOK, dbVM)
	})

	if affectedWorker != "" {
		notifyContext, cancel := context.WithTimeout(ctx.Request.Context(), time.Second)
		defer cancel()

		if err := controller.workerNotifier.Notify(notifyContext, affectedWorker, &rpc.WatchInstruction{
			Action: &rpc.WatchInstruction_SyncVmsAction{},
		}); err != nil {
			controller.logger.Warnf("failed to reactively sync updated VM %s on worker %s: %v",
				name, affectedWorker, err)
		}
	}

	return response
}

func (controller *Controller) updateVMState(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleComputeWrite); responder != nil {
		return responder
	}

	var userVM v1.VM

	if err := ctx.ShouldBindJSON(&userVM); err != nil {
		return responder.JSON(http.StatusBadRequest, NewErrorResponse("invalid JSON was provided"))
	}

	name := ctx.Param("name")

	return controller.storeUpdate(func(txn storepkg.Transaction) responder.Responder {
		dbVM, err := txn.GetVM(name)
		if err != nil {
			return responder.Error(err)
		}

		if dbVM.TerminalState() && dbVM.Status != userVM.Status {
			return responder.JSON(http.StatusPreconditionFailed,
				NewErrorResponse("cannot update status for a VM in a terminal state"))
		}

		if userVM.Status == v1.VMStatusRunning && dbVM.StartedAt.IsZero() {
			dbVM.StartedAt = time.Now()
		}

		dbVM.Status = userVM.Status
		dbVM.StatusMessage = userVM.StatusMessage
		dbVM.ImageFQN = userVM.ImageFQN
		dbVM.VMState = userVM.VMState

		if err := txn.SetVM(*dbVM); err != nil {
			controller.logger.Errorf("failed to update VM in the DB: %v", err)

			return responder.Code(http.StatusInternalServerError)
		}

		return responder.JSON(http.StatusOK, dbVM)
	})
}

func (controller *Controller) getVM(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleComputeRead); responder != nil {
		return responder
	}

	name := ctx.Param("name")

	if ctx.Query("watch") == "true" {
		ctx.Header("Content-Type", "application/x-ndjson")

		watchCh, errCh, err := controller.store.WatchVM(ctx, name)
		if err != nil {
			return responder.Error(err)
		}

		for {
			select {
			case watchMessage := <-watchCh:
				jsonBytes, err := json.Marshal(watchMessage)
				if err != nil {
					controller.logger.Errorf("failed to marshal watch message "+
						"for VM %q to JSON: %v", name, err)

					return responder.Empty()
				}

				if _, err = ctx.Writer.Write(jsonBytes); err != nil {
					return responder.Empty()
				}
				if _, err := ctx.Writer.WriteString("\n"); err != nil {
					return responder.Empty()
				}
				ctx.Writer.Flush()
			case err := <-errCh:
				controller.logger.Errorf("failed to watch VM %q in the DB: %v", name, err)

				return responder.Empty()
			case <-ctx.Done():
				return responder.Empty()
			}
		}
	}

	return controller.storeView(func(txn storepkg.Transaction) responder.Responder {
		vm, err := txn.GetVM(name)
		if err != nil {
			return responder.Error(err)
		}

		return responder.JSON(http.StatusOK, vm)
	})
}

func (controller *Controller) listVMs(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleComputeRead); responder != nil {
		return responder
	}

	var filters []v1.Filter

	if filterRaw := ctx.Query("filter"); filterRaw != "" {
		for _, filterRaw := range strings.Split(filterRaw, ",") {
			filter, err := v1.NewFilter(filterRaw)
			if err != nil {
				return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("%v", err))
			}

			filters = append(filters, filter)
		}
	}

	resultCh := controller.single.DoChan("list-vms", func() (interface{}, error) {
		var vms []v1.VM

		viewErr := controller.store.View(func(txn storepkg.Transaction) (err error) {
			vms, err = txn.ListVMs()
			return
		})

		return vms, viewErr
	})

	var computedVMs interface{}
	var err error

	select {
	case <-ctx.Done():
		return responder.Empty()
	case result := <-resultCh:
		computedVMs = result.Val
		err = result.Err
	}

	if err != nil {
		return responder.Error(err)
	}

	allVMs, ok := computedVMs.([]v1.VM)
	if !ok {
		controller.logger.Errorf("failed to compute vms: %T", computedVMs)
		return responder.Code(http.StatusInternalServerError)
	}

	// Declare an empty, non-nil slice to
	// return [] when no objects are found
	vms := []v1.VM{}

Outer:
	// Use index-based loop to avoid per-iteration copies of v1.VM
	for i := range allVMs {
		for _, filter := range filters {
			if !allVMs[i].Match(filter) {
				continue Outer
			}
		}
		vms = append(vms, allVMs[i])
	}

	return responder.JSON(http.StatusOK, vms)
}

func (controller *Controller) deleteVM(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleComputeWrite); responder != nil {
		return responder
	}

	name := ctx.Param("name")

	return controller.storeUpdate(func(txn storepkg.Transaction) responder.Responder {
		vm, err := txn.GetVM(name)
		if err != nil {
			return responder.Error(err)
		}
		err = txn.DeleteVM(name)
		if err != nil {
			return responder.Error(err)
		}
		err = txn.DeleteEvents("vms", vm.UID)
		if err != nil {
			return responder.Error(err)
		}

		lifecycle.Report(vm, "VM deleted", controller.logger)

		return responder.Code(http.StatusOK)
	})
}

func (controller *Controller) appendVMEvents(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleComputeWrite); responder != nil {
		return responder
	}

	var events []v1.Event

	if err := ctx.ShouldBindJSON(&events); err != nil {
		return responder.JSON(http.StatusBadRequest, NewErrorResponse("invalid JSON was provided"))
	}

	name := ctx.Param("name")

	return controller.storeUpdate(func(txn storepkg.Transaction) responder.Responder {
		vm, err := txn.GetVM(name)
		if err != nil {
			return responder.Error(err)
		}
		if err := txn.AppendEvents(events, "vms", vm.UID); err != nil {
			return responder.Error(err)
		}

		return responder.Code(http.StatusOK)
	})
}

func (controller *Controller) listVMEvents(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleComputeRead); responder != nil {
		return responder
	}

	name := ctx.Param("name")
	options, parseResponder := parseListVMEventsOptions(ctx)
	if parseResponder != nil {
		return parseResponder
	}

	return controller.storeView(func(txn storepkg.Transaction) responder.Responder {
		vm, err := txn.GetVM(name)
		if err != nil {
			return responder.Error(err)
		}

		page, err := txn.ListEventsPage(options, "vms", vm.UID)
		if err != nil {
			return responder.Error(err)
		}
		if len(page.NextCursor) != 0 {
			ctx.Header("X-Next-Cursor", encodeEventCursor(page.NextCursor))
		}

		return responder.JSON(http.StatusOK, page.Items)
	})
}

func parseListVMEventsOptions(ctx *gin.Context) (storepkg.ListOptions, responder.Responder) {
	var options storepkg.ListOptions

	limitRaw := ctx.Query("limit")
	orderRaw := ctx.Query("order")
	cursorRaw := ctx.Query("cursor")

	if limitRaw != "" {
		limit, ok := parsePositiveInt(limitRaw)
		if !ok {
			return options, responder.JSON(http.StatusBadRequest,
				NewErrorResponse("invalid limit %q: expected positive integer", limitRaw))
		}
		options.Limit = limit
	}

	if orderRaw != "" {
		order, err := client.ParseLogsOrder(orderRaw)
		if err != nil {
			return options, responder.JSON(http.StatusBadRequest, NewErrorResponse("%s", err))
		}
		options.Order = storepkg.ListOrder(order)
	}

	if cursorRaw != "" {
		cursor, err := decodeEventCursor(cursorRaw)
		if err != nil {
			return options, responder.JSON(http.StatusBadRequest,
				NewErrorResponse("invalid cursor %q", cursorRaw))
		}
		options.Cursor = cursor
	}

	return options, nil
}

func parsePositiveInt(raw string) (int, bool) {
	value, err := strconv.ParseInt(raw, 10, 0)
	if err != nil || value <= 0 {
		return 0, false
	}

	return int(value), true
}

func encodeEventCursor(cursor []byte) string {
	return base64.RawURLEncoding.EncodeToString(cursor)
}

func decodeEventCursor(cursorRaw string) ([]byte, error) {
	cursor, err := base64.RawURLEncoding.DecodeString(cursorRaw)
	if err == nil {
		return cursor, nil
	}

	return base64.URLEncoding.DecodeString(cursorRaw)
}

func (controller *Controller) validateHostDirs(hostDirs []v1.HostDir) responder.Responder {
	if len(hostDirs) == 0 {
		return nil
	}

	if !controller.insecureAllowHostDirs {
		return responder.JSON(
			http.StatusBadRequest,
			NewErrorResponse("host directory sharing is disabled; "+
				"restart the controller with --insecure-allow-host-dirs to enable this unsafe feature"),
		)
	}

	// Retrieve cluster settings
	var clusterSettings *v1.ClusterSettings
	var err error

	err = controller.store.View(func(txn storepkg.Transaction) error {
		clusterSettings, err = txn.GetClusterSettings()

		return err
	})
	if err != nil {
		controller.logger.Errorf("failed to retrieve cluster settings from the DB: %v", err)

		return responder.Code(http.StatusInternalServerError)
	}

	for _, hostDir := range hostDirs {
		if hostDir.Name == "" {
			return responder.JSON(http.StatusBadRequest,
				NewErrorResponse("hostDir volume's \"name\" field cannot be empty"))
		}

		if hostDir.Path == "" {
			return responder.JSON(http.StatusBadRequest,
				NewErrorResponse("hostDir volume's \"path\" field cannot be empty"))
		}

		if !lo.SomeBy(clusterSettings.HostDirPolicies, func(hostDirPolicy v1.HostDirPolicy) bool {
			return hostDirPolicy.Validate(hostDir.Path, hostDir.ReadOnly)
		}) {
			return responder.JSON(http.StatusBadRequest, NewErrorResponse("host directory %q is disallowed "+
				"by policy, check your cluster settings", hostDir.String()))
		}
	}

	return nil
}
