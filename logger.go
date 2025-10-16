package libschema

import (
	"github.com/muir/testinglogur"

	"github.com/muir/libschema/internal"
)

type Logur interface {
	Trace(msg string, fields ...map[string]interface{})
	Debug(msg string, fields ...map[string]interface{})
	Info(msg string, fields ...map[string]interface{})
	Warn(msg string, fields ...map[string]interface{})
	Error(msg string, fields ...map[string]interface{})
}

// LogFromLogur creates a logger for libschema from a logger that
// implments the recommended interface in Logur.
// See https://github.com/logur/logur#readme
func LogFromLogur(logur Logur) *internal.Log {
	return &internal.Log{
		Logur: logur,
	}
}

// LogFromPrintln creates a logger for libchema from a logger that implements
// Println() like the standard "log" pacakge.
//
//	import "log"
//
//	LogFromPrintln(log.Default())
func LogFromPrintln(printer interface{ Println(...interface{}) }) *internal.Log {
	return LogFromLog(printlnToLog{printer})
}

type printlnToLog struct {
	println interface{ Println(...interface{}) }
}

func (p printlnToLog) Log(v ...interface{}) {
	p.println.Println(v...)
}

// LogFromPrintln creates a logger for libschema from a logger that implements
// Log() like testing.T does.
func LogFromLog(logger interface{ Log(...interface{}) }) *internal.Log {
	return &internal.Log{
		Logur: testinglogur.Get(logger),
	}
}
