package handler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/api/iterator"
)

type NormalSearchHandler struct {
	gcsClient    GCSClient
	bucketName   string
	metadataKeys []string
}

func NewNormalSearchHandler(gcsClient GCSClient, bucketName string, metadataKeys []string) *NormalSearchHandler {
	return &NormalSearchHandler{
		gcsClient:    gcsClient,
		bucketName:   bucketName,
		metadataKeys: metadataKeys,
	}
}

type NormalSearchResult struct {
	ObjectPath string
}

func (nsr NormalSearchResult) Out() {
	fmt.Println(nsr.ObjectPath)
}

func (h *NormalSearchHandler) Do() (*[]NormalSearchResult, error) {
	ctx := context.Background()

	itr := h.gcsClient.Objects(ctx, h.bucketName)
	matchedObjectPaths := make([]NormalSearchResult, 0)

	for {
		objAttrs, err := itr.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			slog.Error("failed to iterate objects", slog.String("error", err.Error()))
			return nil, err
		}
		allExists := true
		for _, metadataKey := range h.metadataKeys {
			if _, exists := objAttrs.Metadata[metadataKey]; !exists {
				allExists = false
			}
		}
		if allExists {
			matchedObjectPaths = append(matchedObjectPaths, NormalSearchResult{ObjectPath: objAttrs.Name})
		}
	}
	return &matchedObjectPaths, nil
}
