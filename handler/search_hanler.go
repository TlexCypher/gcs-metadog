package handler

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"github.com/samber/lo"
	"google.golang.org/api/iterator"
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

func (h *SearchHandler) Do(ctx context.Context, client *storage.Client) (*[]SearchResult, error) {
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
