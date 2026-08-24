package v1

import (
	"errors"
	"fmt"
	"maps"
	"strconv"
)

var ErrInvalidResourceValue = errors.New("resource values should be 64-bit unsigned integers")

const (
	ResourceTartVMs      = "org.cirruslabs.tart-vms"
	ResourceLogicalCores = "org.cirruslabs.logical-cores"
	ResourceMemoryMiB    = "org.cirruslabs.memory-mib"
)

type Resources map[string]uint64

func NewResourcesFromStringToString(
	stringToStringResources map[string]string,
) (Resources, error) {
	result := make(Resources)

	for key, value := range stringToStringResources {
		typedValue, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidResourceValue, err)
		}

		result[key] = typedValue
	}

	return result, nil
}

func (resources Resources) Copy() Resources {
	result := make(Resources)

	for key, value := range resources {
		result[key] = value
	}

	return result
}

func (resources Resources) Add(other Resources) {
	for otherKey, otherValue := range other {
		resources[otherKey] += otherValue
	}
}

func (resources Resources) Added(other Resources) Resources {
	result := resources.Copy()

	for otherKey, otherValue := range other {
		result[otherKey] += otherValue
	}

	return result
}

func (resources Resources) Subtract(other Resources) {
	for otherKey, otherValue := range other {
		if otherValue >= resources[otherKey] {
			resources[otherKey] = 0
			continue
		}

		resources[otherKey] -= otherValue
	}
}

func (resources Resources) Subtracted(other Resources) Resources {
	result := resources.Copy()
	result.Subtract(other)

	return result
}

func (resources Resources) Merge(other Resources) {
	for otherKey, otherValue := range other {
		resources[otherKey] = otherValue
	}
}

func (resources Resources) Merged(other Resources) Resources {
	result := resources.Copy()

	for otherKey, otherValue := range other {
		result[otherKey] = otherValue
	}

	return result
}

func (resources Resources) CanFit(other Resources) bool {
	for otherKey, otherValue := range other {
		if otherValue > resources[otherKey] {
			return false
		}
	}

	return true
}

func (resources Resources) Equal(other Resources) bool {
	return maps.Equal(resources, other)
}
