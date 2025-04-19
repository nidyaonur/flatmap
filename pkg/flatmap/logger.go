package flatmap

// LogLevel defines severity.
type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

// Logger is our minimal logging interface.
type Logger interface {
	Printf(format string, v ...interface{})
}

type noLogger struct{}

func (noLogger) Printf(_ string, _ ...interface{}) {}

// logf prints only if msgLevel >= threshold.
func (fm *FlatNode[K, VT, V, VList]) logf(msgLevel LogLevel, format string, v ...interface{}) {
	if msgLevel < fm.conf.LogLevel {
		return
	}
	// prepend a level tag
	var tag string
	switch msgLevel {
	case DebugLevel:
		tag = "[DEBUG] "
	case InfoLevel:
		tag = "[INFO] "
	case WarnLevel:
		tag = "[WARN] "
	case ErrorLevel:
		tag = "[ERROR] "
	}
	fm.conf.Logger.Printf(tag+format, v...)
}
