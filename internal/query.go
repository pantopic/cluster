package internal

type queryFunc func(*Result, *State, []byte)

var queries = map[byte]queryFunc{
	QUERY_CLUSTER_GET:    queryClusterGet,
	QUERY_CLUSTER_LIST:   queryClusterList,
	QUERY_COMPONENT_GET:  queryComponentGet,
	QUERY_COMPONENT_LIST: queryComponentList,
	QUERY_NAMESPACE_GET:  queryNamespaceGet,
	QUERY_NAMESPACE_LIST: queryNamespaceList,
	QUERY_RESOURCE_GET:   queryResourceGet,
	QUERY_RESOURCE_LIST:  queryResourceList,
	QUERY_SERVICE_GET:    queryServiceGet,
	QUERY_SERVICE_LIST:   queryServiceList,
}
