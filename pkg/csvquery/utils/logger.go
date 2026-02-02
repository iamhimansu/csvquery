package utils

import (
	"log"
	"os"
)

type Logger interface {
	Info(format string, v ...interface{})
	Error(format string, v ...interface{})
	Debug(format string, v ...interface{})
}

type StandardLogger struct {
	logger  *log.Logger
	verbose bool
}

func NewStandardLogger(verbose bool) *StandardLogger {
	return &StandardLogger{
		logger:  log.New(os.Stderr, "[csvquery] ", log.LstdFlags),
		verbose: verbose,
	}
}

func (l *StandardLogger) Info(format string, v ...interface{}) {
	l.logger.Printf("INFO: "+format, v...)
}

func (l *StandardLogger) Error(format string, v ...interface{}) {
	l.logger.Printf("ERROR: "+format, v...)
}

func (l *StandardLogger) Debug(format string, v ...interface{}) {
	if l.verbose {
		l.logger.Printf("DEBUG: "+format, v...)
	}
}
