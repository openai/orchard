package controller

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/cirruslabs/orchard/api"
	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/responder"
	v1pkg "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/deckarep/golang-set/v2"
	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"github.com/go-openapi/runtime/middleware"
	"github.com/samber/lo"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/zap"
)

const ctxServiceAccountKey = "service-account"

var ErrUnauthorized = errors.New("unauthorized")

func (controller *Controller) initAPI() *gin.Engine {
	ginEngine := gin.New()

	// Enable Gin's context fallback to make (*gin.Context).Done() work
	// and to avoid calling the (*gin.Context).Request.Context().Done()
	ginEngine.ContextWithFallback = true

	var group *gin.RouterGroup

	if controller.apiPrefix != "" {
		group = ginEngine.Group(controller.apiPrefix)
	} else {
		group = ginEngine.Group("/")
	}

	group.Use(
		ginzap.Ginzap(controller.logger.Desugar(), "", true),
		ginzap.RecoveryWithZap(controller.logger.Desugar(), true),
		// Expose metrics
		otelgin.Middleware(""),
	)

	// v1 API
	v1 := group.Group("/v1")

	// Auth
	v1.Use(controller.authenticateMiddleware)

	// OpenAPI docs/spec (if enabled) and a way to for the clients
	// to check that the API is working
	v1.GET("/", func(c *gin.Context) {
		if controller.enableSwaggerDocs {
			apiURL := &url.URL{
				Path: "/",
			}
			apiURL = apiURL.JoinPath(controller.apiPrefix, "v1")

			middleware.SwaggerUI(middleware.SwaggerUIOpts{
				Path:    apiURL.Path,
				SpecURL: apiURL.JoinPath("openapi.yaml").Path,
			}, nil).ServeHTTP(c.Writer, c.Request)
		} else {
			c.Status(http.StatusOK)
		}
	})
	if controller.enableSwaggerDocs {
		v1.GET("/openapi.yaml", func(c *gin.Context) {
			c.Data(200, "text/yaml", api.Spec)
		})
	}

	// Controller information
	v1.GET("/controller/info", func(c *gin.Context) {
		controller.controllerInfo(c).Respond(c)
	})

	// Cluster settings
	v1.GET("/cluster-settings", func(c *gin.Context) {
		controller.getClusterSettings(c).Respond(c)
	})
	v1.PUT("/cluster-settings", func(c *gin.Context) {
		controller.updateClusterSettings(c).Respond(c)
	})

	// Service accounts
	v1.POST("/service-accounts", func(c *gin.Context) {
		controller.createServiceAccount(c).Respond(c)
	})
	v1.PUT("/service-accounts/:name", func(c *gin.Context) {
		controller.updateServiceAccount(c).Respond(c)
	})
	v1.GET("/service-accounts/:name", func(c *gin.Context) {
		controller.getServiceAccount(c).Respond(c)
	})
	v1.GET("/service-accounts", func(c *gin.Context) {
		controller.listServiceAccounts(c).Respond(c)
	})
	v1.DELETE("/service-accounts/:name", func(c *gin.Context) {
		controller.deleteServiceAccount(c).Respond(c)
	})

	// Workers
	v1.POST("/workers", func(c *gin.Context) {
		controller.createWorker(c).Respond(c)
	})
	v1.PUT("/workers/:name", func(c *gin.Context) {
		controller.updateWorker(c).Respond(c)
	})
	v1.GET("/workers/:name", func(c *gin.Context) {
		controller.getWorker(c).Respond(c)
	})
	v1.GET("/workers", func(c *gin.Context) {
		controller.listWorkers(c).Respond(c)
	})
	v1.GET("/workers/:name/port-forward", func(c *gin.Context) {
		controller.portForwardWorker(c).Respond(c)
	})
	v1.DELETE("/workers/:name", func(c *gin.Context) {
		controller.deleteWorker(c).Respond(c)
	})

	// RPC v2
	v1.GET("/rpc/watch", func(c *gin.Context) {
		controller.rpcWatch(c).Respond(c)
	})
	v1.GET("/rpc/port-forward", func(c *gin.Context) {
		controller.rpcPortForward(c).Respond(c)
	})
	v1.POST("/rpc/resolve-ip", func(c *gin.Context) {
		controller.rpcResolveIP(c).Respond(c)
	})

	// VMs
	v1.POST("/vms", func(c *gin.Context) {
		controller.createVM(c).Respond(c)
	})
	v1.PUT("/vms/:name", func(c *gin.Context) {
		if strings.HasPrefix(c.GetHeader("User-Agent"), "Orchard/0") {
			// Backward compatibility for older Orchard Workers that still
			// use the PUT /vms/{name} API endpoint to update a VM status
			//
			// Note that we include the "0" here to avoid targeting users
			// of the github.com/cirruslabs/orchard/pkg/client package. For
			// them, the UA string should normally be "Orchard/unknown-unknown".
			//
			// After some months/years we can remove this workaround and at
			// the very worst the workers simply won't progress with the VMs
			// assigned to them. An upgrade to a newer version will fix that.
			controller.updateVMState(c).Respond(c)
		} else {
			controller.updateVMSpec(c).Respond(c)
		}
	})
	v1.PUT("/vms/:name/state", func(c *gin.Context) {
		controller.updateVMState(c).Respond(c)
	})
	v1.GET("/vms/:name", func(c *gin.Context) {
		controller.getVM(c).Respond(c)
	})
	v1.GET("/vms", func(c *gin.Context) {
		controller.listVMs(c).Respond(c)
	})
	v1.GET("/vms/:name/port-forward", func(c *gin.Context) {
		controller.portForwardVM(c).Respond(c)
	})
	v1.GET("/vms/:name/exec", func(c *gin.Context) {
		controller.execVM(c).Respond(c)
	})
	v1.GET("/vms/:name/ip", func(c *gin.Context) {
		controller.ip(c).Respond(c)
	})
	v1.DELETE("/vms/:name", func(c *gin.Context) {
		controller.deleteVM(c).Respond(c)
	})
	v1.GET("/vms/:name/events", func(c *gin.Context) {
		controller.listVMEvents(c).Respond(c)
	})
	v1.POST("/vms/:name/events", func(c *gin.Context) {
		controller.appendVMEvents(c).Respond(c)
	})

	return ginEngine
}

func (controller *Controller) fetchServiceAccount(name string, token string) (*v1pkg.ServiceAccount, error) {
	var serviceAccount *v1pkg.ServiceAccount
	var err error

	err = controller.store.View(func(txn storepkg.Transaction) error {
		serviceAccount, err = txn.GetServiceAccount(name)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, storepkg.ErrNotFound) {
			return nil, ErrUnauthorized
		}

		return nil, err
	}

	if subtle.ConstantTimeCompare([]byte(serviceAccount.Token), []byte(token)) == 0 {
		return nil, ErrUnauthorized
	}

	return serviceAccount, nil
}

func (controller *Controller) authenticateMiddleware(c *gin.Context) {
	// Retrieve presented credentials (if any)
	user, password, ok := c.Request.BasicAuth()
	if !ok {
		c.Next()

		return
	}

	serviceAccount, err := controller.fetchServiceAccount(user, password)
	if err != nil {
		if errors.Is(err, ErrUnauthorized) {
			responder.Code(http.StatusUnauthorized).Respond(c)
		} else {
			responder.Error(err).Respond(c)
		}

		return
	}

	// Remember service account for further authorize() calls
	c.Set(ctxServiceAccountKey, serviceAccount)

	c.Next()
}

type AuthorizeMode int

const (
	AuthorizeModeAll AuthorizeMode = iota
	AuthorizeModeAny
)

func (controller *Controller) authorize(
	ctx *gin.Context,
	requiredRoles ...v1pkg.ServiceAccountRole,
) responder.Responder {
	return controller.authorizeBase(ctx, AuthorizeModeAll, requiredRoles...)
}

func (controller *Controller) authorizeAny(
	ctx *gin.Context,
	requiredRoles ...v1pkg.ServiceAccountRole,
) responder.Responder {
	return controller.authorizeBase(ctx, AuthorizeModeAny, requiredRoles...)
}

func (controller *Controller) authorizeBase(
	ctx *gin.Context,
	mode AuthorizeMode,
	requiredRoles ...v1pkg.ServiceAccountRole,
) responder.Responder {
	if controller.insecureAuthDisabled {
		return nil
	}

	serviceAccountUntyped, ok := ctx.Get(ctxServiceAccountKey)
	if !ok {
		return responder.Code(http.StatusUnauthorized)
	}
	serviceAccount := serviceAccountUntyped.(*v1pkg.ServiceAccount)
	serviceAccountRolesSet := mapset.NewSet[v1pkg.ServiceAccountRole](serviceAccount.Roles...)

	var authorized bool

	switch mode {
	case AuthorizeModeAll:
		authorized = serviceAccountRolesSet.Contains(requiredRoles...)
	case AuthorizeModeAny:
		authorized = serviceAccountRolesSet.ContainsAny(requiredRoles...)
	}

	if authorized {
		return nil
	}

	var hint string

	switch mode {
	case AuthorizeModeAll:
		hint = "all of the following roles must be present"
	case AuthorizeModeAny:
		hint = "any of the following roles must be present"
	}

	humanizedRoles := lo.Map(requiredRoles, func(role v1pkg.ServiceAccountRole, _ int) string {
		return string(role)
	})

	return responder.JSON(http.StatusUnauthorized,
		NewErrorResponse("%s: %s", hint, strings.Join(humanizedRoles, ", ")))
}

type storeTransactionFunc func(operation func(txn storepkg.Transaction) error) error

func (controller *Controller) storeView(view func(txn storepkg.Transaction) responder.Responder) responder.Responder {
	return adaptResponderToStoreOperation(controller.logger, controller.store.View, view)
}

func (controller *Controller) storeUpdate(
	update func(txn storepkg.Transaction) responder.Responder,
) responder.Responder {
	return adaptResponderToStoreOperation(controller.logger, controller.store.Update, update)
}

func adaptResponderToStoreOperation(
	logger *zap.SugaredLogger,
	storeOperation storeTransactionFunc,
	responderOperation func(txn storepkg.Transaction) responder.Responder,
) responder.Responder {
	var result responder.Responder

	if err := storeOperation(func(txn storepkg.Transaction) error {
		result = responderOperation(txn)

		return nil
	}); err != nil {
		logger.Errorf("encountered an error during store operation: %v", err)

		return responder.Code(http.StatusInternalServerError)
	}

	return result
}
