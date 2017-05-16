package logger

import (
	"os"

	log "github.com/Sirupsen/logrus"
)

func Initialize() {

	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(customFormatter)
	customFormatter.FullTimestamp = true
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)

}
