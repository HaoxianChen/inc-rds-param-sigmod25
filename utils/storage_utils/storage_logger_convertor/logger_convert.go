package logger_convert

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/storage/config"
	"gitlab.grandhoo.com/rock/storage/config/log_config"
	"strings"
)

type RockLogger struct {
}

func (RockLogger) WriteString(level log_config.LogLevel, msg string) {
	msg = strings.TrimSpace(msg)
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(msg)
		}
	}()

	//// 监控数据用 debug
	//if strings.Contains(msg, "monitor") {
	//	logger.Debug(msg)
	//}

	switch level {
	case log_config.Debug:
		logger.Debug(msg)
	case log_config.Info:
		logger.Info(msg)
	case log_config.Warn:
		logger.Warn(msg)
	case log_config.Error:
		logger.Error(msg)
	default:
		logger.Info(msg)
	}
}
func (RockLogger) Flush() {

}

func init() {
	config.LoggerConfig(log_config.Config{
		CallerBack:     2,
		Level:          log_config.Debug,
		DebugMode:      false,
		TimeInfoFlag:   false,
		LevelInfoFlag:  false,
		ServiceName:    "",
		CallerInfoFlag: false,
	})
	config.LoggerConfig(log_config.Config{
		CallerBack:     2,
		Level:          log_config.Info,
		DebugMode:      false,
		TimeInfoFlag:   false,
		LevelInfoFlag:  false,
		ServiceName:    "",
		CallerInfoFlag: true,
	})
	config.LoggerConfig(log_config.Config{
		CallerBack:     2,
		Level:          log_config.Warn,
		DebugMode:      false,
		TimeInfoFlag:   false,
		LevelInfoFlag:  false,
		ServiceName:    "storage",
		CallerInfoFlag: true,
	})
	config.LoggerConfig(log_config.Config{
		CallerBack:     2,
		Level:          log_config.Error,
		DebugMode:      false,
		TimeInfoFlag:   false,
		LevelInfoFlag:  false,
		ServiceName:    "storage",
		CallerInfoFlag: true,
	})
}
