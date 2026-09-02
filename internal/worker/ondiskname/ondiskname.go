package ondiskname

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
)

var (
	ErrNotManagedByOrchard = errors.New("this on-disk VM is not managed by Orchard")
	ErrInvalidOnDiskName   = errors.New("invalid on-disk VM name")
)

const (
	prefix = "orchard"

	numPartsPrefix       = 1
	numPartsName         = 1
	numPartsUUID         = 5
	numPartsRestartCount = 1
	numPartsTotal        = numPartsPrefix + numPartsName + numPartsUUID + numPartsRestartCount
)

type OnDiskName struct {
	Name         string
	UID          string
	RestartCount uint64
}

func New(name string, uid string, restartCount uint64) OnDiskName {
	return OnDiskName{
		Name:         name,
		UID:          uid,
		RestartCount: restartCount,
	}
}

func NewFromResource(vm v1.VM) OnDiskName {
	return OnDiskName{
		Name:         vm.Name,
		UID:          vm.UID,
		RestartCount: vm.RestartCount,
	}
}

func Parse(s string) (OnDiskName, error) {
	splits := strings.Split(s, "-")

	if !strings.HasPrefix(s, fmt.Sprintf("%s-", prefix)) {
		return OnDiskName{}, ErrNotManagedByOrchard
	}

	if len(splits) < numPartsTotal {
		return OnDiskName{}, fmt.Errorf("%w: name should contain at least %d parts delimited by \"-\"",
			ErrInvalidOnDiskName, numPartsTotal)
	}

	if splits[0] != prefix {
		return OnDiskName{}, fmt.Errorf("%w: name should begin with \"%s\" prefix",
			ErrInvalidOnDiskName, prefix)
	}

	uuidStart := len(splits) - numPartsUUID - numPartsRestartCount

	restartCountRaw := splits[uuidStart+numPartsUUID]
	restartCount, err := strconv.ParseUint(restartCountRaw, 10, 64)
	if err != nil {
		return OnDiskName{}, fmt.Errorf("%w: invalid restart count %q",
			ErrInvalidOnDiskName, restartCountRaw)
	}

	return OnDiskName{
		Name:         strings.Join(splits[1:uuidStart], "-"),
		UID:          strings.Join(splits[uuidStart:uuidStart+numPartsUUID], "-"),
		RestartCount: restartCount,
	}, nil
}

func (odn OnDiskName) String() string {
	return fmt.Sprintf("%s-%s-%s-%d", prefix, odn.Name, odn.UID, odn.RestartCount)
}

func (odn OnDiskName) ControlSocketPath() (string, error) {
	// Try user-overridden TART_HOME first
	tartHome := os.Getenv("TART_HOME")
	if tartHome == "" {
		// Fall back to default TART_HOME
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to determine user home directory: %w", err)
		}

		tartHome = filepath.Join(homeDir, ".tart")
	}

	return filepath.Join(tartHome, "vms", odn.String(), "control.sock"), nil
}
