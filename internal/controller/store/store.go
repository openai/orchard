package store

import (
	"context"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
)

type WatchMessageType string

const (
	WatchMessageTypeAdded    WatchMessageType = "ADDED"
	WatchMessageTypeModified WatchMessageType = "MODIFIED"
	WatchMessageTypeDeleted  WatchMessageType = "DELETED"
)

type WatchMessage[T any] struct {
	Type   WatchMessageType `json:"type,omitempty"`
	Object T                `json:"object,omitempty"`
}

type Store interface {
	View(cb func(txn Transaction) error) error
	Update(cb func(txn Transaction) error) error
	WatchVM(ctx context.Context, vmName string) (chan WatchMessage[v1.VM], chan error, error)
	Close() error
}

type Transaction interface {
	GetVM(name string) (result *v1.VM, err error)
	SetVM(vm v1.VM) (err error)
	DeleteVM(name string) (err error)
	ListVMs() (result []v1.VM, err error)

	GetWorker(name string) (result *v1.Worker, err error)
	SetWorker(worker v1.Worker) (err error)
	DeleteWorker(name string) (err error)
	ListWorkers() (result []v1.Worker, err error)

	GetServiceAccount(name string) (result *v1.ServiceAccount, err error)
	SetServiceAccount(serviceAccount *v1.ServiceAccount) (err error)
	DeleteServiceAccount(name string) (err error)
	ListServiceAccounts() (result []v1.ServiceAccount, err error)

	AppendEvents(event []v1.Event, scope ...string) (err error)
	ListEvents(scope ...string) (result []v1.Event, err error)
	ListEventsPage(options ListOptions, scope ...string) (result Page[v1.Event], err error)
	DeleteEvents(scope ...string) (err error)

	GetClusterSettings() (*v1.ClusterSettings, error)
	SetClusterSettings(clusterSettings v1.ClusterSettings) error
}

type ListOptions struct {
	Limit  int
	Cursor []byte
	Order  ListOrder
}

type Page[T any] struct {
	Items      []T
	NextCursor []byte
}

type ListOrder string

const (
	ListOrderAsc  ListOrder = "asc"
	ListOrderDesc ListOrder = "desc"
)
