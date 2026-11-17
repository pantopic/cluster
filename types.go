package cluster

import (
	"github.com/pantopic/cluster/internal"
)

var (
	ShardID = internal.ShardID

	Status_Pending     = internal.Status_Pending
	Status_Active      = internal.Status_Active
	Status_Unavailable = internal.Status_Unavailable
	Status_Stopped     = internal.Status_Stopped
	Status_Gone        = internal.Status_Gone

	ResultSuccess           = internal.ResultSuccess
	ResultErrAuthentication = internal.ResultErrAuthentication
	ResultErrAuthorization  = internal.ResultErrAuthorization
	ResultErrConflict       = internal.ResultErrConflict
	ResultErrExists         = internal.ResultErrExists
	ResultErrInternal       = internal.ResultErrInternal
	ResultErrNotFound       = internal.ResultErrNotFound
	ResultErrSemantic       = internal.ResultErrSemantic
	ResultErrSyntactic      = internal.ResultErrSyntactic
)
