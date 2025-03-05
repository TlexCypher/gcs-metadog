package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/samber/lo"
	"google.golang.org/api/iterator"
	"log/slog"
	"strings"
)

type NestSearchHandler struct {
	gcsClient  GCSClient
	bucketName string
	dependTask string
	parameters map[string]string
}

func NewNestSearchHandler(gcsClient GCSClient, bucketName string, dependTask string, parameters map[string]string) *NestSearchHandler {
	return &NestSearchHandler{
		gcsClient:  gcsClient,
		bucketName: bucketName,
		dependTask: dependTask,
		parameters: parameters,
	}
}

type NestSearchResult struct {
	ObjectPath string
}

func (nsr NestSearchResult) Out() {
	fmt.Println(nsr.ObjectPath)
}

// TODO: refactor
func (h *NestSearchHandler) Do() (*[]NestSearchResult, error) {
	ctx := context.Background()

	itr := h.gcsClient.Objects(ctx, h.bucketName)
	matchedObjectPaths := make([]NestSearchResult, 0)

	for {
		objAttrs, err := itr.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			slog.Error("failed to iterate objects", slog.String("error", err.Error()))
			return nil, err
		}
		requiredTaskPathsJsonStr := objAttrs.Metadata["required_task_outputs"]
		if requiredTaskPathsJsonStr != "" {
			var taskNameVsOutputPathMap map[string]string
			if err := json.Unmarshal([]byte(requiredTaskPathsJsonStr), &taskNameVsOutputPathMap); err != nil {
				slog.Error("failed to unmarshal required_task_paths", slog.String("error", err.Error()))
				return nil, err
			}
			if outputPath, ok := taskNameVsOutputPathMap[h.dependTask]; ok {
				bucketName, err := getBucketNameFromGCSPath(outputPath)
				if err != nil {
					slog.Error("failed to get bucket name", slog.String("error", err.Error()))
					return nil, err
				}
				nitr := h.gcsClient.Objects(ctx, bucketName)
				for {
					nObjAttrs, err := nitr.Next()
					if errors.Is(err, iterator.Done) {
						break
					}
					if err != nil {
						slog.Error("failed to iterate objects", slog.String("error", err.Error()))
						return nil, err
					}
					matchedFlag := true
					for paramName, paramValue := range h.parameters {
						value, ok := nObjAttrs.Metadata[paramName]
						if !ok {
							matchedFlag = false
							break
						}
						if paramValue != value {
							matchedFlag = false
							break
						}
					}
					if matchedFlag {
						matchedObjectPaths = append(matchedObjectPaths, NestSearchResult{ObjectPath: outputPath})
					}
				}
			}
		}
	}
	return lo.ToPtr(matchedObjectPaths), nil
}

func getBucketNameFromGCSPath(gcsPath string) (string, error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return "", fmt.Errorf("invalid gcs path: %s", gcsPath)
	}
	pathWOSchema := strings.TrimPrefix(gcsPath, "gs://")
	parts := strings.SplitN(pathWOSchema, "/", 2)
	return "gs://" + parts[0], nil
}
