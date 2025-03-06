package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"

	"gcs-metadog/handler"
	"github.com/samber/lo"
	"github.com/urfave/cli/v2"
)

const (
	ExitCodeOK  int = 0
	ExitCodeErr int = iota
)

const (
	NestMode string = "nest"
)

const (
	Parameter   string = "parameter"
	DependTask  string = "dependTask"
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
		Flags:  buildAllFlags(),
		Before: validateFlags,
		Action: run,
	}
	return app
}

func buildAllFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:    NestMode,
			Aliases: []string{"n"},
			Usage:   "Nest mode",
		},
		&cli.StringFlag{
			Name:     Bucket,
			Aliases:  []string{"b"},
			Usage:    "GCS bucket name",
			Required: true,
		},
		&cli.StringFlag{
			Name:    DependTask,
			Aliases: []string{"t"},
			Usage:   "Task name that is registered as a dependency of other tasks",
		},
		&cli.StringSliceFlag{
			Name:    Parameter,
			Aliases: []string{"p"},
			Usage:   "Specify key=value pairs (can be used multiple times)",
		},
		&cli.StringSliceFlag{
			Name:    MetadataKey,
			Aliases: []string{"m"},
			Usage:   "Specify metadata key (can be used multiple times)",
		},
	}
}

func validateFlags(cCtx *cli.Context) error {
	if cCtx.Bool(NestMode) {
		if cCtx.IsSet(MetadataKey) {
			return errors.New("metadata key is not allowed in nest mode")
		} else if !cCtx.IsSet(DependTask) || !cCtx.IsSet(Parameter) {
			return errors.New("depend-task and parameter are required in nest mode")
		}
	} else {
		if cCtx.IsSet(DependTask) || cCtx.IsSet(Parameter) {
			return errors.New("depend-task and parameter are allowed in nest mode")
		} else if !cCtx.IsSet(MetadataKey) {
			return errors.New("metadata key is required in normal mode")
		}
	}
	return nil
}

func run(cCtx *cli.Context) error {
	if !cCtx.Bool(NestMode) {
		return runWithNormalMode(cCtx)
	} else {
		return runWithNestMode(cCtx)
	}
}

func runWithNormalMode(cCtx *cli.Context) error {
	ctx := context.Background()
	gcsClient, err := handler.NewRealGCSClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize GCS client: %w", err)
	}
	defer gcsClient.Close()

	bucket := cCtx.String(Bucket)
	slog.Info("confirm gcs bucket name: ", slog.String("gcs-bucket-name", bucket))

	metadataKeys := cCtx.StringSlice(MetadataKey)
	lo.ForEach(metadataKeys, func(metadataKey string, index int) {
		slog.Info("confirm each metadata key: ", slog.String("gcs-metadata-key", metadataKey))
	})

	sh := handler.NewNormalSearchHandler(gcsClient, bucket, metadataKeys)

	srs, err := sh.Do()
	if err != nil {
		return fmt.Errorf("failed to search such objects: %w", err)
	}

	lo.ForEach(*srs, func(sr handler.NormalSearchResult, index int) {
		sr.Out()
	})
	return nil
}

func runWithNestMode(cCtx *cli.Context) error {
	ctx := context.Background()
	gcsClient, err := handler.NewRealGCSClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize GCS client: %w", err)
	}
	defer gcsClient.Close()

	bucket := cCtx.String(Bucket)
	slog.Info("confirm gcs bucket name: ", slog.String("gcs-bucket-name", bucket))

	dependTask := cCtx.String(DependTask)
	parameters := cCtx.StringSlice(Parameter)
	parametersMap, err := getParameterMap(parameters)
	if err != nil {
		slog.Error("invalid parameter expression", slog.String("parameter", "parameter's format should be key=value."))
		return err
	}
	nsh := handler.NewNestSearchHandler(*gcsClient, bucket, dependTask, parametersMap)

	nsrs, err := nsh.Do()
	if err != nil {
		return fmt.Errorf("failed to search such objects: %w", err)
	}
	if nsrs == nil {
		return nil
	}
	lo.ForEach(lo.Uniq(nsrs), func(sr handler.NestSearchResult, index int) {
		sr.Out()
	})
	return nil
}

func getParameterMap(parameters []string) (map[string]string, error) {
	parameterMap := make(map[string]string, 0)
	for _, parameterExp := range parameters {
		parts := strings.Split(parameterExp, "=")
		keyValueCounts := 2
		if len(parts) != keyValueCounts {
			return nil, fmt.Errorf("invalid parameter expression: %s", parameterExp)
		}
		parameterMap[parts[0]] = parts[1]
	}
	return parameterMap, nil
}
