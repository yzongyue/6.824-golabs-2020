package utils

import "log"

// Custom leveled logger
type MyLogger struct {
	Level LogLevel // this need to be exported?
	Module string
}

func (l *MyLogger) LevelToStr() string {
	if l.Level == DEBUG {
		return "DEBUG"
	} else if l.Level == INFO {
		return "INFO"
	} else {
		return "ERROR"
	}
}

func (l *MyLogger) Debug(msg string) {
	if l.Level <= DEBUG {
		log.Printf("[%s] [%s] %s", l.Module, "DEBUG", msg)
	}
}

func (l *MyLogger) Info(msg string) {
	if l.Level <= INFO {
		log.Printf("[%s] [%s] %s", l.Module, "INFO", msg)
	}
}

func (l *MyLogger) Error(msg string) {
	if l.Level <= ERROR {
		log.Printf("[%s] [%s] %s", l.Module, "ERROR", msg)
	}
}

type LogLevel int
const (
	DEBUG LogLevel = iota
	INFO
	ERROR
)
const (
	DEFAULT_LOG_LEVEL = DEBUG
)