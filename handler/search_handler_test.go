package handler

import (
	"context"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

type MockObjectIterator struct {
	objects []*storage.ObjectAttrs
	index   int
}

func (m *MockObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if m.index >= len(m.objects) {
		return nil, iterator.Done
	}
	obj := m.objects[m.index]
	m.index++
	return obj, nil
}

type MockGCSClient struct {
	mockIterator ObjectIterator
}

func (m *MockGCSClient) Objects(_ context.Context, _ string) ObjectIterator {
	return m.mockIterator
}

func (m *MockGCSClient) Close() error {
	return nil
}

func TestSearchHandler_Do(t *testing.T) {
	t.Parallel()
	mockObjects := []*storage.ObjectAttrs{
		{Name: "file1.txt", Metadata: map[string]string{"key1": "value1"}},
		{Name: "file2.txt", Metadata: map[string]string{"key2": "value2"}},
		{Name: "file3.txt", Metadata: map[string]string{"key1": "value3"}},
	}
	mockItr := &MockObjectIterator{objects: mockObjects}
	mockClient := &MockGCSClient{mockIterator: mockItr}
	handler := NewSearchHandler(mockClient, "test-bucket", []string{"key1"})

	results, err := handler.Do()
	require.NoError(t, err)

	expectedResults := []SearchResult{
		{ObjectPath: "file1.txt"},
		{ObjectPath: "file3.txt"},
	}
	assert.Equal(t, expectedResults, *results)
}
