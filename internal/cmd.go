package internal

type cmdFunc func(*Entry, *State, []byte) error

var commands = map[byte]cmdFunc{
	CMD_CLUSTER_PUT:   cmdClusterPut,
	CMD_NAMESPACE_PUT: cmdNamespacePut,
	CMD_RESOURCE_PUT:  cmdResourcePut,
	CMD_COMPONENT_PUT: cmdComponentPut,
	CMD_SERVICE_PUT:   cmdServicePut,
}
