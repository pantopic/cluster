package internal

const (
	ResultSuccess uint64 = iota
	ResultErrSyntactic
	ResultErrSemantic
	ResultErrAuthentication
	ResultErrAuthorization
	ResultErrNotFound
	ResultErrConflict
	ResultErrInternal
)
