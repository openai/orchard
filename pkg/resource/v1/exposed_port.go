package v1

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrInvalidExposedPort = errors.New("invalid exposed port specification")
	errPortOutOfRange     = errors.New("port must be in the range 1-65535")
)

// ExposedPort is a parsed netSoftnetExpose entry.
type ExposedPort struct {
	External uint16
	Internal uint16
}

// NewExposedPortFromString parses an EXTERNAL:INTERNAL entry.
//
// Orchard is stricter than Softnet here: it rejects port 0,
// which cannot be exposed, and a leading sign.
func NewExposedPortFromString(s string) (ExposedPort, error) {
	splits := strings.Split(s, ":")
	if len(splits) != 2 {
		return ExposedPort{}, fmt.Errorf("%w %q: the format should be EXTERNAL:INTERNAL", ErrInvalidExposedPort, s)
	}

	external, err := parsePort(splits[0])
	if err != nil {
		return ExposedPort{}, fmt.Errorf("%w %q: invalid external port: %w", ErrInvalidExposedPort, s, err)
	}

	internal, err := parsePort(splits[1])
	if err != nil {
		return ExposedPort{}, fmt.Errorf("%w %q: invalid internal port: %w", ErrInvalidExposedPort, s, err)
	}

	return ExposedPort{External: external, Internal: internal}, nil
}

func parsePort(s string) (uint16, error) {
	port, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0, err
	}

	if port == 0 {
		return 0, errPortOutOfRange
	}

	return uint16(port), nil
}
