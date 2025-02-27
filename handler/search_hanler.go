package handler

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"github.com/samber/lo"
	"google.golang.org/api/iterator"
	"log/slog"
)

type SearchHandler struct {
	bucketName   string
	metadataKeys []string
}

type SearchResult struct {
	ObjectPath string
}

func (sr *SearchResult) Out() {
	fmt.Println(sr.ObjectPath)
}

func NewSearchHandler(bucketName string, metadataKeys []string) *SearchHandler {
	return &SearchHandler{
		bucketName:   bucketName,
		metadataKeys: metadataKeys,
	}
}

func (h *SearchHandler) Do() (*[]SearchResult, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		slog.Error("failed to initialize storage client", slog.String("error", err.Error()))
		return nil, err
	}
	defer client.Close()

	itr := client.Bucket(h.bucketName).Objects(ctx, nil)
	matchedObjectPaths := make([]SearchResult, 0)
	for {
		objAttrs, err := itr.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		for _, metadataKey := range h.metadataKeys {
			if _, exists := objAttrs.Metadata[metadataKey]; exists {
				matchedObjectPaths = append(matchedObjectPaths, SearchResult{ObjectPath: objAttrs.Name})
			}
		}
	}
	return lo.ToPtr(matchedObjectPaths), nil
}
