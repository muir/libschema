package libschema_test

import (
	"log"
	"testing"

	"github.com/muir/libschema"

	"github.com/muir/testinglogur"
)

// mostly this just needs to compile
func LogFromThings(t *testing.T) {
	_ = libschema.LogFromPrintln(log.Default())
	_ = libschema.LogFromLog(t)
	log := libschema.LogFromLogur(testinglogur.Get(t))

	log.Trace("trace")
	log.Debug("debug")
	log.Info("info")
	log.Warn("warn")
	log.Error("error")
}
