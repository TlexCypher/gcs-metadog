package handler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/api/iterator"
)

type SearchHandler struct {
	gcsClient    GCSClient
	bucketName   string
	metadataKeys []string
}

func NewSearchHandler(gcsClient GCSClient, bucketName string, metadataKeys []string) *SearchHandler {
	return &SearchHandler{
		gcsClient:    gcsClient,
		bucketName:   bucketName,
		metadataKeys: metadataKeys,
	}
}

type SearchResult struct {
	ObjectPath string
}

func (sr SearchResult) Out() {
	fmt.Println(sr.ObjectPath)
}

func (h *SearchHandler) Do() (*[]SearchResult, error) {
	ctx := context.Background()

	itr := h.gcsClient.Objects(ctx, h.bucketName) // GCSClient 経由で Objects() を取得
	matchedObjectPaths := make([]SearchResult, 0)

	for {
		objAttrs, err := itr.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			slog.Error("failed to iterate objects", slog.String("error", err.Error()))
			return nil, err
		}
		for _, metadataKey := range h.metadataKeys {
			if _, exists := objAttrs.Metadata[metadataKey]; exists {
				matchedObjectPaths = append(matchedObjectPaths, SearchResult{ObjectPath: objAttrs.Name})
			}
		}
	}
	return &matchedObjectPaths, nil
}
