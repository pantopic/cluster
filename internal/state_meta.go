package internal

// Index returns the last committed index of the statemachine.
func (s *State) Index() (val uint64) {
	return s.metaGetVal(`index`)
}

type metaValue struct {
	Key  string
	Val  uint64
	Data []byte
}

func (s *State) metaGet(key string) (val uint64, data []byte) {
	res, _ := s.txn.First(`meta`, `id`, key)
	if res != nil {
		val = res.(metaValue).Val
		data = res.(metaValue).Data
	}
	return
}

func (s *State) metaSet(key string, val uint64, data []byte) {
	err := s.txn.Insert(`meta`, metaValue{key, val, data})
	if err != nil {
		panic(err)
	}
}

func (s *State) metaSetIndex(val uint64) {
	s.metaSet(`index`, val, nil)
}

func (s *State) metaGetVal(key string) (val uint64) {
	val, _ = s.metaGet(key)
	return
}

func (s *State) metaIncrVal(key string) uint64 {
	val := s.metaGetVal(key)
	val++
	s.metaSet(key, val, nil)
	return val
}

func (s *State) metaGetDataString(key string) string {
	_, b := s.metaGet(key)
	return string(b)
}
