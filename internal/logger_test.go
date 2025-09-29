package internal

import "testing"

type fakeLogger struct{ calls []string }

func (f *fakeLogger) Trace(msg string, _ ...map[string]interface{}) { f.calls = append(f.calls, "trace:"+msg) }
func (f *fakeLogger) Debug(msg string, _ ...map[string]interface{}) { f.calls = append(f.calls, "debug:"+msg) }
func (f *fakeLogger) Info(msg string, _ ...map[string]interface{})  { f.calls = append(f.calls, "info:"+msg) }
func (f *fakeLogger) Warn(msg string, _ ...map[string]interface{})  { f.calls = append(f.calls, "warn:"+msg) }
func (f *fakeLogger) Error(msg string, _ ...map[string]interface{}) { f.calls = append(f.calls, "error:"+msg) }

func TestLogPassThrough(t *testing.T) {
    fl := &fakeLogger{}
    l := &Log{Logur: fl}
    l.Trace("a")
    l.Debug("b")
    l.Info("c")
    l.Warn("d")
    l.Error("e")
    want := []string{"trace:a", "debug:b", "info:c", "warn:d", "error:e"}
    if len(fl.calls) != len(want) {
        t.Fatalf("expected %d calls got %d", len(want), len(fl.calls))
    }
    for i := range want {
        if fl.calls[i] != want[i] {
            t.Fatalf("call %d got %q want %q", i, fl.calls[i], want[i])
        }
    }
}
