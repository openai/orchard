package v1

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidHostDirPolicy = errors.New("invalid hostDir policy")

type HostDirPolicy struct {
	PathPrefix string `json:"pathPrefix,omitempty"`
	ReadOnly   bool   `json:"ro,omitempty"`
}

func NewHostDirPolicyFromString(s string) (HostDirPolicy, error) {
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		return HostDirPolicy{
			PathPrefix: strings.TrimSuffix(s, ":ro"),
			ReadOnly:   strings.HasSuffix(s, ":ro"),
		}, nil
	}

	parts := strings.Split(s, ":")

	if len(parts) > 2 {
		return HostDirPolicy{}, fmt.Errorf("%w: hostDir policy should contain 2 parts at max, found %d",
			ErrInvalidHostDirPolicy, len(parts))
	}

	if parts[0] == "" {
		return HostDirPolicy{}, fmt.Errorf("%w: path prefix cannot be empty", ErrInvalidHostDirPolicy)
	}

	var readOnly bool

	if len(parts) == 2 {
		if parts[1] == "ro" {
			readOnly = true
		} else {
			return HostDirPolicy{}, fmt.Errorf("%w: hostDir policy's second part can only be \"ro\", found %q",
				ErrInvalidHostDirPolicy, parts[1])
		}
	}

	return HostDirPolicy{
		PathPrefix: parts[0],
		ReadOnly:   readOnly,
	}, nil
}

func (policy HostDirPolicy) Validate(path string, readOnly bool) bool {
	if strings.Contains(path, "..") {
		return false
	}

	if policy.ReadOnly && !readOnly {
		return false
	}

	path = strings.TrimSuffix(path, "/")
	pathPrefix := strings.TrimSuffix(policy.PathPrefix, "/")

	return path == pathPrefix || strings.HasPrefix(path, pathPrefix+"/")
}

func (policy HostDirPolicy) String() string {
	var roPart string

	if policy.ReadOnly {
		roPart = ":ro"
	}

	return fmt.Sprintf("%s%s", policy.PathPrefix, roPart)
}
