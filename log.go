package main

import (
	"btcd-demo/btclog"
	"fmt"
	"github.com/jrick/logrotate/rotator"
	"os"
	"path/filepath"
)

var (
	logRotator *rotator.Rotator
)

type Level uint32

type sLoger struct {
}

var (
	btcdLog = new(sLoger)
)

var subsystemLoggers = map[string]btclog.Logger{}

// initLogRotator initializes the logging rotator to writer logs to logFile and
// create roll files in the same directory. It must be called before the
// package-global log rotater variables are used.
func initLogRotator(logFile string) {
	logDir, _ := filepath.Split(logFile)
	err := os.MkdirAll(logDir, 0700)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log directory: %v\n", err)
		os.Exit(1)
	}
	r, err := rotator.New(logFile, 10*1024, false, 3)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create file rotator: %v\n", err)
		os.Exit(1)
	}
	logRotator = r
}

func (l *sLoger) Tracef(format string, params ...interface{}) {

}

func (l *sLoger) Debugf(format string, params ...interface{}) {

}

// Infof formats message according to format specifier and writes to
// log with LevelInfo.
func (l *sLoger) Infof(format string, params ...interface{}) {
	fmt.Printf(format, params)
	fmt.Println()
}

// Warnf formats message according to format specifier and writes to
// to log with LevelWarn.
func (l *sLoger) Warnf(format string, params ...interface{}) {

}

// Errorf formats message according to format specifier and writes to
// to log with LevelError.
func (l *sLoger) Errorf(format string, params ...interface{}) {
	fmt.Printf(format, params)
	fmt.Println()
}

// Criticalf formats message according to format specifier and writes to
// log with LevelCritical.
func (l *sLoger) Criticalf(format string, params ...interface{}) {

}

// Trace formats message using the default formats for its operands
// and writes to log with LevelTrace.
func (l *sLoger) Trace(v ...interface{}) {

}

// Debug formats message using the default formats for its operands
// and writes to log with LevelDebug.
func (l *sLoger) Debug(v ...interface{}) {

}

// Info formats message using the default formats for its operands
// and writes to log with LevelInfo.
func (l *sLoger) Info(v ...interface{}) {
	fmt.Printf("%s\n", v)
}

// Warn formats message using the default formats for its operands
// and writes to log with LevelWarn.
func (l *sLoger) Warn(v ...interface{}) {

}

// Error formats message using the default formats for its operands
// and writes to log with LevelError.
func (l *sLoger) Error(v ...interface{}) {

}

// Critical formats message using the default formats for its operands
// and writes to log with LevelCritical.
func (l *sLoger) Critical(v ...interface{}) {

}

// Level returns the current logging level.
func (l *sLoger) Level() Level {
	return 0
}

// SetLevel changes the logging level to the passed level.
func (l *sLoger) SetLevel(level Level) {

}

func setLogLevels(logLevel string) {
	for subSystemID := range subsystemLoggers {
		setLogLevel(subSystemID, logLevel)
	}
}

func setLogLevel(subsytemID string, logLevel string) {
	// Ignore invalid subsystems
	logger, ok := subsystemLoggers[subsytemID]
	if !ok {
		return
	}

	// Defaults to info if the log level is invalid
	level, _ := btclog.LevelFromString(logLevel)
	logger.SetLevel(level)
}
