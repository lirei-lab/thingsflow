package main

import (
	"log/slog"
	"testing"
)

func TestAlarmEventLogLevelDefaultsToInfo(t *testing.T) {
	t.Setenv("FLOW_ALARM_EVENT_LOG_LEVEL", "")

	if got := alarmEventLogLevel(); got != slog.LevelInfo {
		t.Fatalf("alarmEventLogLevel() = %v, want %v", got, slog.LevelInfo)
	}
}

func TestAlarmEventLogLevelCanBeDebug(t *testing.T) {
	t.Setenv("FLOW_ALARM_EVENT_LOG_LEVEL", "debug")

	if got := alarmEventLogLevel(); got != slog.LevelDebug {
		t.Fatalf("alarmEventLogLevel() = %v, want %v", got, slog.LevelDebug)
	}
}
