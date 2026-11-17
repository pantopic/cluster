package cluster

import (
	"github.com/logbn/zongzi"
)

type Logger zongzi.Logger

func getLogger(cfg Config) Logger {
	log := zongzi.GetLogger(`pantopic`)
	switch cfg.LogLevel {
	case "debug":
		zongzi.SetLogLevelDebug()
		log.SetLevel(zongzi.LogLevelDebug)
	case "info":
		zongzi.SetLogLevelProduction()
		log.SetLevel(zongzi.LogLevelInfo)
	case "warn":
		zongzi.SetLogLevelProduction()
		log.SetLevel(zongzi.LogLevelWarning)
	}
	return log
}
