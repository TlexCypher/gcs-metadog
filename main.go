package main

import (
	"log/slog"
	"os"

	"gcs-metadog/cmd"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	os.Exit(cmd.Core())
}
