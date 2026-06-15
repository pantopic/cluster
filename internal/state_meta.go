package internal

func (fsm *State) Index() (val uint64) {
	return fsm.metaGetVal(`index`)
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

func (fsm *State) metaIncrVal(key string) uint64 {
	val := fsm.metaGetVal(key)
	val++
	fsm.metaSet(key, val, nil)
	return val
}

func (fsm *State) metaGetDataString(key string) string {
	_, b := fsm.metaGet(key)
	return string(b)
}
