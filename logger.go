package kv

import (
	"github.com/rs/zerolog"
	"os"
)

const (
	dateFormat = "2006-01-02T15:04:05.000" // YYYY-MM-DDTHH:MM:SS.ZZZ
)

var zlog zerolog.Logger

func init() {
	loggerWritter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: dateFormat,
	}

	zlog = zerolog.New(loggerWritter)
	zlog.Level(zerolog.DebugLevel)
}

type (
	Logger interface {
		Errorf(string, ...any)
		Warningf(string, ...any)
		Infof(string, ...any)
		Debugf(string, ...any)
	}

	badgerLogger struct {
		fileLog bool
		zlog    zerolog.Logger
	}
)

func (b badgerLogger) Errorf(format string, v ...any) {
	b.zlog.Error().Msgf(format, v...)
}

func (b badgerLogger) Warningf(format string, v ...any) {
	b.zlog.Warn().Msgf(format, v...)
}

func (b badgerLogger) Infof(format string, v ...any) {
	b.zlog.Info().Msgf(format, v...)
}

func (b badgerLogger) Debugf(format string, v ...any) {
	b.zlog.Debug().Msgf(format, v...)
}
