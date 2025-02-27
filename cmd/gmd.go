package cmd

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"gcs-metadog/handler"
	"github.com/samber/lo"
	"github.com/urfave/cli/v2"
)

const (
	ExitCodeOK  int = 0
	ExitCodeErr int = iota
)

const (
	Bucket      string = "bucket"
	MetadataKey string = "metadata"
)

func Core() int {
	/* context would be closed, when this process catch os.Interrupt. */
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	/* NOTE:
	The reason why I don't use Run(os.Args), because of explicit cancellation with context.
	I use github.com/urfave/cli/v2, but app.Run() interface is not good.
	*/
	app := newApp()
	if err := app.RunContext(ctx, os.Args); err != nil {
		fmt.Println(err)
		return ExitCodeErr
	}
	return ExitCodeOK
}

func newApp() *cli.App {
	app := &cli.App{
		Name:   "gmd (gcs-metadog)",
		Usage:  "A command-line tool for searching GCS object which has exact metadata.",
		Action: run,
		Flags:  buildFlags(),
	}
	return app
}

func buildFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    Bucket,
			Aliases: []string{"b"},
			Usage:   "GCS bucket name.",
		},
		&cli.StringSliceFlag{
			Name:    MetadataKey,
			Aliases: []string{"m"},
			Usage:   "GCS metadata key.",
		},
	}
}

func newGCSClient() (*storage.Client, context.Context, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, ctx, err
	}
	return client, ctx, nil
}

func run(cCtx *cli.Context) error {
	bucket := cCtx.String(Bucket)
	slog.Info("confirm gcs bucket name: ", slog.String("gcs-bucket-name", bucket))

	metadataKeys := cCtx.StringSlice(MetadataKey)
	lo.ForEach(metadataKeys, func(metadataKey string, index int) {
		slog.Info("confirm each metadata key: ", slog.String("gcs-metadata-key", metadataKey))
	})

	sh := handler.NewSearchHandler(bucket, metadataKeys)
	client, ctx, err := newGCSClient()
	if err != nil {
		slog.Error("failed to initialize storage client", slog.String("error", err.Error()))
	}
	srs, err := sh.Do(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to search such objects: %w", err)
	}
	lo.ForEach(*srs, func(sr handler.SearchResult, index int) {
		sr.Out()
	})
	return nil
}
