package internal

type queryFunc func(*Result, *State, []byte)

var queries = map[byte]queryFunc{
	QUERY_CLUSTER_GET:   queryClusterGet,
	QUERY_NAMESPACE_GET: queryNamespaceGet,
	QUERY_RESOURCE_GET:  queryResourceGet,
	QUERY_COMPONENT_GET: queryComponentGet,
	QUERY_SERVICE_GET:   queryServiceGet,
}
