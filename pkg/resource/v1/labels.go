package v1

import "maps"

const (
	LabelWorkerName = "org.cirruslabs.orchard.worker-name"
)

type Labels map[string]string

func (labels Labels) Contains(other Labels) bool {
	for label, value := range other {
		if labels[label] != value {
			return false
		}
	}

	return true
}

func (labels Labels) Copy() Labels {
	if labels == nil {
		return make(Labels)
	}

	return maps.Clone(labels)
}

func (labels Labels) Merged(other Labels) Labels {
	result := labels.Copy()

	maps.Copy(result, other)

	return result
}
