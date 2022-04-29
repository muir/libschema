package libschema_test

import (
	"log"
	"testing"

	"github.com/muir/libschema"

	"github.com/muir/testinglogur"
)

type p struct {
	l interface{ Log(...interface{}) }
}

func (p p) Println(v ...interface{}) {
	p.l.Log(v...)
}

// mostly this just needs to compile
func TestLogFromThings(t *testing.T) {
	_ = libschema.LogFromLogur(testinglogur.Get(t))
	_ = libschema.LogFromLog(t)
	_ = libschema.LogFromPrintln(log.Default())
	log := libschema.LogFromPrintln(p{t})

	log.Trace("trace")
	log.Debug("debug")
	log.Info("info")
	log.Warn("warn")
	log.Error("error")
}
