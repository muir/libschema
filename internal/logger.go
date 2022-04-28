package internal

type Logur interface {
	Trace(msg string, fields ...map[string]interface{})
	Debug(msg string, fields ...map[string]interface{})
	Info(msg string, fields ...map[string]interface{})
	Warn(msg string, fields ...map[string]interface{})
	Error(msg string, fields ...map[string]interface{})
}

type Log struct {
	Logur Logur
}

func (log *Log) Trace(msg string, fields ...map[string]interface{}) { log.Logur.Trace(msg, fields...) }
func (log *Log) Debug(msg string, fields ...map[string]interface{}) { log.Logur.Debug(msg, fields...) }
func (log *Log) Info(msg string, fields ...map[string]interface{})  { log.Logur.Info(msg, fields...) }
func (log *Log) Warn(msg string, fields ...map[string]interface{})  { log.Logur.Warn(msg, fields...) }
func (log *Log) Error(msg string, fields ...map[string]interface{}) { log.Logur.Error(msg, fields...) }
