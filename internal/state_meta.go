package internal

func (fsm *State) Index() (val uint64) {
	return fsm.metaGetVal(`index`)
}

// NamespaceID returns the ID of the last created Namespace
func (fsm *State) NamespaceID() uint64 {
	return fsm.getID(Type_Namespace)
}
func (fsm *State) incrNamespaceID() uint64 {
	return fsm.incrID(Type_Namespace)
}

// ComponentID returns the ID of the last created Component
func (fsm *State) ComponentID() uint64 {
	return fsm.getID(Type_Component)
}
func (fsm *State) incrComponentID() uint64 {
	return fsm.incrID(Type_Component)
}

// ResourceID returns the ID of the last created Resource
func (fsm *State) ResourceID() uint64 {
	return fsm.getID(Type_Resource)
}
func (fsm *State) incrResourceID() uint64 {
	return fsm.incrID(Type_Resource)
}

// ClusterID returns the ID of the last created Cluster
func (fsm *State) ClusterID() uint64 {
	return fsm.getID(Type_Cluster)
}
func (fsm *State) incrClusterID() uint64 {
	return fsm.incrID(Type_Cluster)
}

// ServiceID returns the ID of the last created Service
func (fsm *State) ServiceID() uint64 {
	return fsm.getID(Type_Service)
}
func (fsm *State) incrServiceID() uint64 {
	return fsm.incrID(Type_Service)
}

// EndpointID returns the ID of the last created Endpoint
func (fsm *State) EndpointID() uint64 {
	return fsm.getID(Type_Endpoint)
}
func (fsm *State) incrEndpointID() uint64 {
	return fsm.incrID(Type_Endpoint)
}

func (fsm *State) incrID(t string) uint64 {
	id := fsm.metaGetVal(t + `ID`)
	id++
	fsm.metaSet(t+`ID`, id, nil)
	return id
}

func (fsm *State) getID(t string) uint64 {
	id := fsm.metaGetVal(t + `ID`)
	id++
	fsm.metaSet(t+`ID`, id, nil)
	return id
}

func (fsm *State) metaGet(key string) (val uint64, data []byte) {
	res, _ := fsm.txn.First(`meta`, `id`, key)
	if res != nil {
		val = res.(metaValue).Val
		data = res.(metaValue).Data
	}
	return
}

func (fsm *State) metaSet(key string, val uint64, data []byte) {
	err := fsm.txn.Insert(`meta`, metaValue{key, val, data})
	if err != nil {
		panic(err)
	}
}

func (fsm *State) metaSetIndex(val uint64) {
	fsm.metaSet(`index`, val, nil)
}

func (fsm *State) metaGetVal(key string) (val uint64) {
	val, _ = fsm.metaGet(key)
	return
}

func (fsm *State) metaGetDataString(key string) string {
	_, b := fsm.metaGet(key)
	return string(b)
}
