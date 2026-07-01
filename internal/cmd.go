package internal

type cmdFunc func(*Entry, *State, []byte) error

var commands = map[byte]cmdFunc{
	CMD_CLUSTER_DELETE:   cmdClusterDelete,
	CMD_CLUSTER_PUT:      cmdClusterPut,
	CMD_COMPONENT_DELETE: cmdComponentDelete,
	CMD_COMPONENT_PUT:    cmdComponentPut,
	CMD_NAMESPACE_DELETE: cmdNamespaceDelete,
	CMD_NAMESPACE_PUT:    cmdNamespacePut,
	CMD_RESOURCE_DELETE:  cmdResourceDelete,
	CMD_RESOURCE_PUT:     cmdResourcePut,
	CMD_SERVICE_DELETE:   cmdServiceDelete,
	CMD_SERVICE_PUT:      cmdServicePut,
}
