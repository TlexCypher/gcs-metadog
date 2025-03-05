package handler

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/samber/lo"
	"google.golang.org/api/iterator"
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
		requiredTaskPathsJSONStr, exists := objAttrs.Metadata["required_task_outputs"]
		if !exists {
			continue
		}
		var taskNameVsOutputPathMap map[string]string
		if err := json.Unmarshal([]byte(requiredTaskPathsJSONStr), &taskNameVsOutputPathMap); err != nil {
			slog.Error("failed to unmarshal required_task_paths", slog.String("error", err.Error()))
			return nil, err
		}
		if outputPath, exists := taskNameVsOutputPathMap[h.dependTask]; exists {
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
				}
				matched := h.isMatchedObject(nObjAttrs)
				if matched {
					matchedObjectPaths = append(matchedObjectPaths, NestSearchResult{ObjectPath: outputPath})
				}
			}
		}
	}
	return lo.ToPtr(matchedObjectPaths), nil
}

func (h *NestSearchHandler) isMatchedObject(objAttrs *storage.ObjectAttrs) bool {
	matched := true
	for paramName, paramValue := range h.parameters {
		value, ok := objAttrs.Metadata[paramName]
		if !ok {
			matched = false
			break
		}
		if paramValue != value {
			matched = false
			break
		}
	}
	return matched
}

func getBucketNameFromGCSPath(gcsPath string) (string, error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return "", fmt.Errorf("invalid gcs path: %s", gcsPath)
	}

	pathWOSchema := strings.TrimPrefix(gcsPath, "gs://")
	slashCount := 2
	parts := strings.SplitN(pathWOSchema, "/", slashCount)
	return parts[0], nil
}
