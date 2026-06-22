package controller

import (
	"errors"
	"net/http"
	"time"

	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/responder"
	"github.com/cirruslabs/orchard/internal/simplename"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func (controller *Controller) createServiceAccount(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleAdminWrite); responder != nil {
		return responder
	}

	var serviceAccount v1.ServiceAccount

	if err := ctx.ShouldBindJSON(&serviceAccount); err != nil {
		return responder.JSON(http.StatusBadRequest, NewErrorResponse("invalid JSON was provided"))
	}

	// Validate service account name
	if serviceAccount.Name == "" {
		return responder.JSON(http.StatusPreconditionFailed,
			NewErrorResponse("service account name is empty"))
	} else if err := simplename.Validate(serviceAccount.Name); err != nil {
		return responder.JSON(http.StatusPreconditionFailed,
			NewErrorResponse("service account %v", err))
	}

	// Validate roles
	for _, role := range serviceAccount.Roles {
		_, err := v1.NewServiceAccountRole(string(role))
		if err != nil {
			return responder.JSON(http.StatusPreconditionFailed,
				NewErrorResponse("unsupported role \"%s\"", role))
		}
	}

	if serviceAccount.Token == "" {
		serviceAccount.Token = uuid.New().String()
	}

	serviceAccount.CreatedAt = time.Now()

	return controller.storeUpdate(func(txn storepkg.Transaction) responder.Responder {
		// Does the Service Account resource with this name already exists?
		_, err := txn.GetServiceAccount(serviceAccount.Name)
		if err != nil && !errors.Is(err, storepkg.ErrNotFound) {
			controller.logger.Errorf("failed to check if the service account exists in the DB: %v", err)

			return responder.Code(http.StatusInternalServerError)
		}
		if err == nil {
			return responder.JSON(http.StatusConflict,
				NewErrorResponse("service account with this name already exists"))
		}

		if err := txn.SetServiceAccount(&serviceAccount); err != nil {
			controller.logger.Errorf("failed to create the service account in the DB: %v", err)

			return responder.Code(http.StatusInternalServerError)
		}

		return responder.JSON(http.StatusOK, &serviceAccount)
	})
}

func (controller *Controller) updateServiceAccount(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleAdminWrite); responder != nil {
		return responder
	}

	var userServiceAccount v1.ServiceAccount

	if err := ctx.ShouldBindJSON(&userServiceAccount); err != nil {
		return responder.JSON(http.StatusBadRequest, NewErrorResponse("invalid JSON was provided"))
	}

	name := ctx.Param("name")

	// Validate service account name
	if name == "" {
		return responder.JSON(http.StatusPreconditionFailed,
			NewErrorResponse("service account name is empty"))
	} else if err := simplename.Validate(name); err != nil {
		return responder.JSON(http.StatusPreconditionFailed,
			NewErrorResponse("service account %v", err))
	}
	if userServiceAccount.Name == "" {
		return responder.JSON(http.StatusPreconditionFailed,
			NewErrorResponse("service account name is empty"))
	}
	if userServiceAccount.Name != name {
		return responder.JSON(http.StatusPreconditionFailed,
			NewErrorResponse("service account name does not match URL path"))
	}

	// Validate roles
	for _, role := range userServiceAccount.Roles {
		_, err := v1.NewServiceAccountRole(string(role))
		if err != nil {
			return responder.JSON(http.StatusPreconditionFailed,
				NewErrorResponse("unsupported role \"%s\"", role))
		}
	}

	if userServiceAccount.Token == "" {
		return responder.JSON(http.StatusPreconditionFailed, NewErrorResponse("service account token is empty"))
	}

	return controller.storeUpdate(func(txn storepkg.Transaction) responder.Responder {
		dbServiceAccount, err := txn.GetServiceAccount(name)
		if err != nil {
			return responder.Error(err)
		}

		dbServiceAccount.Token = userServiceAccount.Token
		dbServiceAccount.Roles = userServiceAccount.Roles

		if err := txn.SetServiceAccount(dbServiceAccount); err != nil {
			controller.logger.Errorf("failed to update service account in the DB: %v", err)

			return responder.Code(http.StatusInternalServerError)
		}

		return responder.JSON(http.StatusOK, &dbServiceAccount)
	})
}

func (controller *Controller) getServiceAccount(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleAdminRead); responder != nil {
		return responder
	}

	name := ctx.Param("name")

	return controller.storeView(func(txn storepkg.Transaction) responder.Responder {
		serviceAccount, err := txn.GetServiceAccount(name)
		if err != nil {
			return responder.Error(err)
		}

		return responder.JSON(http.StatusOK, &serviceAccount)
	})
}

func (controller *Controller) listServiceAccounts(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleAdminRead); responder != nil {
		return responder
	}

	return controller.storeView(func(txn storepkg.Transaction) responder.Responder {
		serviceAccounts, err := txn.ListServiceAccounts()
		if err != nil {
			return responder.Error(err)
		}

		return responder.JSON(http.StatusOK, &serviceAccounts)
	})
}

func (controller *Controller) deleteServiceAccount(ctx *gin.Context) responder.Responder {
	if responder := controller.authorize(ctx, v1.ServiceAccountRoleAdminWrite); responder != nil {
		return responder
	}

	name := ctx.Param("name")

	return controller.storeUpdate(func(txn storepkg.Transaction) responder.Responder {
		if err := txn.DeleteServiceAccount(name); err != nil {
			return responder.Error(err)
		}

		return responder.Code(http.StatusOK)
	})
}
