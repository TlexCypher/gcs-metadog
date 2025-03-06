package main

import (
	"log"
	"log/slog"
	"os"

	"gcs-metadog/cmd"
)

const filePermission = 0644

func main() {
	file, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePermission)
	if err != nil {
		log.Fatalf("failed to open app.log file: %s", err)
	}
	defer file.Close()
	logger := slog.New(slog.NewJSONHandler(file, nil))
	slog.SetDefault(logger)

	os.Exit(cmd.Core())
}
