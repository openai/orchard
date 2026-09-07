package etcd

import (
	"path"

	"github.com/cirruslabs/orchard/pkg/resource/v1"
)

const (
	SpaceVMs             = "/vms"
	SpaceWorkers         = "/workers"
	SpaceServiceAccounts = "/service-accounts"
	clusterSettingsKey   = "/cluster-settings"
)

func (store *Store) VMKey(name string) string {
	return store.key(path.Join(SpaceVMs, name))
}

func (txn *Transaction) GetVM(name string) (*v1.VM, error) {
	return genericGet[v1.VM](txn, txn.store.VMKey(name))
}

func (txn *Transaction) SetVM(vm v1.VM) error {
	return genericSet[v1.VM](txn, txn.store.VMKey(vm.Name), vm)
}

func (txn *Transaction) DeleteVM(name string) error {
	return genericDelete(txn, txn.store.VMKey(name))
}

func (txn *Transaction) ListVMs() ([]v1.VM, error) {
	return genericList[v1.VM](txn, SpaceVMs)
}

func (store *Store) workerKey(name string) string {
	return store.key(path.Join(SpaceWorkers, name))
}

func (txn *Transaction) GetWorker(name string) (*v1.Worker, error) {
	return genericGet[v1.Worker](txn, txn.store.workerKey(name))
}

func (txn *Transaction) SetWorker(worker v1.Worker) error {
	return genericSet[v1.Worker](txn, txn.store.workerKey(worker.Name), worker)
}

func (txn *Transaction) DeleteWorker(name string) error {
	return genericDelete(txn, txn.store.workerKey(name))
}

func (txn *Transaction) ListWorkers() ([]v1.Worker, error) {
	return genericList[v1.Worker](txn, SpaceWorkers)
}

func (store *Store) serviceAccountKey(name string) string {
	return store.key(path.Join(SpaceServiceAccounts, name))
}

func (txn *Transaction) GetServiceAccount(name string) (*v1.ServiceAccount, error) {
	return genericGet[v1.ServiceAccount](txn, txn.store.serviceAccountKey(name))
}

func (txn *Transaction) SetServiceAccount(serviceAccount *v1.ServiceAccount) error {
	return genericSet[v1.ServiceAccount](txn, txn.store.serviceAccountKey(serviceAccount.Name), *serviceAccount)
}

func (txn *Transaction) DeleteServiceAccount(name string) error {
	return genericDelete(txn, txn.store.serviceAccountKey(name))
}

func (txn *Transaction) ListServiceAccounts() ([]v1.ServiceAccount, error) {
	return genericList[v1.ServiceAccount](txn, SpaceServiceAccounts)
}

func (txn *Transaction) GetClusterSettings() (*v1.ClusterSettings, error) {
	return genericGet[v1.ClusterSettings](txn, txn.store.key(clusterSettingsKey))
}

func (txn *Transaction) SetClusterSettings(clusterSettings v1.ClusterSettings) error {
	return genericSet[v1.ClusterSettings](txn, txn.store.key(clusterSettingsKey), clusterSettings)
}
