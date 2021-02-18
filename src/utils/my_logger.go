package utils

import "log"

// Custom leveled logger
type MyLogger struct {
	Level LogLevel // this need to be exported?
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
		log.Printf("[%s] %s", "DEBUG", msg)
	}
}

func (l *MyLogger) Info(msg string) {
	if l.Level <= INFO {
		log.Printf("[%s] %s", "INFO", msg)
	}
}

func (l *MyLogger) Error(msg string) {
	if l.Level <= ERROR {
		log.Printf("[%s] %s", "ERROR", msg)
	}
}

type LogLevel int
const (
	DEBUG LogLevel = iota
	INFO
	ERROR
)