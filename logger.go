package main

import (
	"github.com/logbn/zongzi"
)

func getLogger(cfg config) zongzi.Logger {
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
