package internal

func validateFieldPresence(entry *Entry, fields ...string) bool {
	for i := 0; i < len(fields); i += 2 {
		name := fields[i]
		val := fields[i+1]
		if len(val) == 0 {
			entry.Result.Value = ResultErrSyntactic
			entry.Result.Data = []byte(name + ` cannot be empty`)
			return false
		}
	}
	return true
}
