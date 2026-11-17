package v1

import (
	"encoding/base64"
	"errors"
)

type WireGuardSpec struct {
	// Required IPv4 overlay address for guest NAT
	Address string `json:"address"`

	// Omit peers or send an empty list to withdraw the peer
	Peers []WireGuardPeer `json:"peers,omitzero"`

	// One IPv4 prefix reserved for the tunnel, including after withdrawal
	Routes []string `json:"routes"`

	// Optional inner IPv4 MTU for the tunnel route
	MTU *uint16 `json:"mtu,omitempty"`
}

type WireGuardPeer struct {
	PublicKey                  string   `json:"publicKey"`
	Endpoint                   string   `json:"endpoint"`
	AllowedIPs                 []string `json:"allowedIPs"`
	PersistentKeepaliveSeconds uint16   `json:"persistentKeepaliveSeconds,omitempty"`
}

type WireGuardStatus struct {
	PublicKey  string `json:"publicKey"`
	ListenPort uint16 `json:"listenPort"`
}

func (wireGuard WireGuardSpec) Validate() error {
	if len(wireGuard.Peers) > 1 {
		return errors.New("at most one WireGuard peer is supported")
	}
	if wireGuard.Address == "" {
		return errors.New("WireGuard address is required")
	}
	if len(wireGuard.Routes) != 1 || wireGuard.Routes[0] == "" {
		return errors.New("routes must contain exactly one IPv4 prefix")
	}

	for _, peer := range wireGuard.Peers {
		if err := peer.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (peer WireGuardPeer) Validate() error {
	if peer.PublicKey == "" {
		return errors.New("peer public key is required")
	}
	if peer.Endpoint == "" {
		return errors.New("peer endpoint is required")
	}
	if len(peer.AllowedIPs) != 1 || peer.AllowedIPs[0] == "" {
		return errors.New("peer allowedIPs must contain exactly one IPv4 prefix")
	}

	return nil
}

func (status WireGuardStatus) Validate() error {
	if status.ListenPort == 0 {
		return errors.New("WireGuard's listen port must be non-zero")
	}
	if !validWireGuardPublicKey(status.PublicKey) {
		return errors.New("invalid WireGuard public key")
	}

	return nil
}

func validWireGuardPublicKey(key string) bool {
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(raw) != 32 || base64.StdEncoding.EncodeToString(raw) != key {
		return false
	}

	return [32]byte(raw) != [32]byte{}
}
