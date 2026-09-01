package controller

import (
	"crypto/tls"
	"time"

	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

type Option func(*Controller)

func WithDataDir(dataDir *DataDir) Option {
	return func(controller *Controller) {
		controller.dataDir = dataDir
	}
}

func WithListenAddr(listenAddr string) Option {
	return func(controller *Controller) {
		controller.listenAddr = listenAddr
	}
}

func WithAPIPrefix(apiPrefix string) Option {
	return func(c *Controller) {
		c.apiPrefix = apiPrefix
	}
}

func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(controller *Controller) {
		controller.tlsConfig = tlsConfig
	}
}

func WithSSHServer(listenAddr string, signer ssh.Signer, noClientAuth bool) Option {
	return func(controller *Controller) {
		controller.sshListenAddr = listenAddr
		controller.sshSigner = signer
		controller.sshNoClientAuth = noClientAuth
	}
}

func WithInsecureAuthDisabled() Option {
	return func(controller *Controller) {
		controller.insecureAuthDisabled = true
	}
}

func WithInsecureAllowHostDirs() Option {
	return func(controller *Controller) {
		controller.insecureAllowHostDirs = true
	}
}

func WithSwaggerDocs() Option {
	return func(controller *Controller) {
		controller.enableSwaggerDocs = true
	}
}

func WithWorkerOfflineTimeout(workerOfflineTimeout time.Duration) Option {
	return func(controller *Controller) {
		controller.workerOfflineTimeout = workerOfflineTimeout
	}
}

func WithExecSessionRetentionTTL(execSessionRetentionTTL time.Duration) Option {
	return func(controller *Controller) {
		controller.execSessionRetentionTTL = execSessionRetentionTTL
	}
}

func WithExecSSHConnectionKeepaliveInterval(execSSHConnectionKeepaliveInterval time.Duration) Option {
	return func(controller *Controller) {
		controller.execSSHConnectionKeepaliveInterval = execSSHConnectionKeepaliveInterval
	}
}

func WithDisableDBCompression() Option {
	return func(controller *Controller) {
		controller.disableDBCompression = true
	}
}

func WithPingInterval(pingInterval time.Duration) Option {
	return func(controller *Controller) {
		controller.pingInterval = pingInterval
	}
}

func WithSynthetic() Option {
	return func(controller *Controller) {
		controller.synthetic = true
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(controller *Controller) {
		controller.logger = logger.Sugar()
	}
}
