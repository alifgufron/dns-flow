package logger

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"

	"golang.org/x/term"
)

type Level string

const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

// Init creates a slog.Logger.
// When stdout is a TTY (interactive terminal), human-readable text format is used.
// When stdout is not a TTY (e.g., redirected to a log file by daemon(8)), JSON
// format is used so every line carries a full timestamp, level, and message.
func Init(level string, serviceName string) *slog.Logger {
	logLevel := parseLevel(Level(strings.ToLower(level)))

	opts := &slog.HandlerOptions{
		Level: logLevel,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.SourceKey {
				return slog.Attr{}
			}
			return a
		},
	}

	var handler slog.Handler
	if term.IsTerminal(int(os.Stdout.Fd())) {
		// Interactive terminal: use human-readable text format.
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		// Non-interactive (service / log file): use JSON for structured logs.
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	logger := slog.New(handler).With("service", serviceName)
	slog.SetDefault(logger)

	// Redirect stdlib log to slog so any third-party log.Printf calls are captured.
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC)

	logger.Info("initialized", "level", level)

	return logger
}

func parseLevel(level Level) slog.Level {
	switch level {
	case LevelDebug:
		return slog.LevelDebug
	case LevelInfo:
		return slog.LevelInfo
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func Fatal(logger *slog.Logger, msg string, args ...any) {
	logger.Error(fmt.Sprintf("FATAL: %s", msg), args...)
	os.Exit(1)
}
