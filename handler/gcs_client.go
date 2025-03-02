package handler

import (
	"context"

	"cloud.google.com/go/storage"
)

type GCSClient interface {
	Objects(ctx context.Context, bucketName string) ObjectIterator
	Close() error
}

type ObjectIterator interface {
	Next() (*storage.ObjectAttrs, error)
}

type RealGCSClient struct {
	client *storage.Client
}

func NewRealGCSClient(ctx context.Context) (*RealGCSClient, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return &RealGCSClient{client: client}, nil
}

func (r *RealGCSClient) Objects(ctx context.Context, bucketName string) ObjectIterator {
	return r.client.Bucket(bucketName).Objects(ctx, nil)
}

func (r *RealGCSClient) Close() error {
	return r.client.Close()
}
