package worker

import (
	"github.com/cirruslabs/orchard/internal/dialer"
	"github.com/cirruslabs/orchard/internal/worker/runtime"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/samber/mo"
	"go.uber.org/zap"
)

type Option func(*Worker)

func WithName(name string) Option {
	return func(worker *Worker) {
		worker.name = name
	}
}

func WithNameSuffix(nameSuffix string) Option {
	return func(worker *Worker) {
		worker.nameSuffix = nameSuffix
	}
}

func WithResources(resources v1.Resources) Option {
	return func(worker *Worker) {
		worker.resources = resources
	}
}

func WithLabels(labels v1.Labels) Option {
	return func(worker *Worker) {
		worker.labels = labels
	}
}

func WithDefaultCPUAndMemory(defaultCPU uint64, defaultMemory uint64) Option {
	return func(worker *Worker) {
		worker.defaultCPU = defaultCPU
		worker.defaultMemory = defaultMemory
	}
}

func WithDialer(dialer dialer.Dialer) Option {
	return func(worker *Worker) {
		worker.dialer = dialer
	}
}

func WithSynthetic() Option {
	return func(worker *Worker) {
		worker.runtime = runtime.NewSynthetic()
	}
}

func WithSoftnetPolicyUpdates(enabled bool) Option {
	return func(worker *Worker) {
		worker.softnetPolicyUpdates = mo.Some(enabled)
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(worker *Worker) {
		worker.logger = logger.Sugar()
	}
}
