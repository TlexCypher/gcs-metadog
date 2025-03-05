package handler

import (
	"context"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
)

type mockGCSClient struct {
	objects map[string][]*storage.ObjectAttrs
}

func (m *mockGCSClient) Objects(ctx context.Context, bucket string) ObjectIterator {
	objAttrs, exists := m.objects[bucket]
	if !exists {
		return &mockObjectIterator{objects: []*storage.ObjectAttrs{}}
	}
	return &mockObjectIterator{objects: objAttrs}
}

func (m *mockGCSClient) Close() error {
	return nil
}

type mockObjectIterator struct {
	objects []*storage.ObjectAttrs
	index   int
}

func (m *mockObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if m.index >= len(m.objects) {
		return nil, iterator.Done
	}
	obj := m.objects[m.index]
	m.index++
	return obj, nil
}

func TestNestSearchHandler_Do(t *testing.T) {
	tests := []struct {
		name        string
		gcsObjects  map[string][]*storage.ObjectAttrs
		dependTask  string
		parameters  map[string]string
		expected    []NestSearchResult
		expectError bool
	}{
		{
			name:       "Valid case",
			dependTask: "task1",
			parameters: map[string]string{"paramA": "valueA"},
			gcsObjects: map[string][]*storage.ObjectAttrs{
				"test-bucket": {
					{
						Name: "file1.txt",
						Metadata: map[string]string{
							"required_task_outputs": `{"task1": "gs://output-bucket/task1-result"}`,
						},
					},
				},
				"output-bucket": {
					{
						Name: "task1-result",
						Metadata: map[string]string{
							"paramA": "valueA",
						},
					},
				},
			},
			expected:    []NestSearchResult{{ObjectPath: "gs://output-bucket/task1-result"}},
			expectError: false,
		},
		{
			name:       "No dependent task",
			dependTask: "task2",
			parameters: map[string]string{"paramA": "valueA"},
			gcsObjects: map[string][]*storage.ObjectAttrs{
				"test-bucket": {
					{
						Name: "file1.txt",
						Metadata: map[string]string{
							"required_task_outputs": `{"task1": "gs://output-bucket/task1-result"}`,
						},
					},
				},
			},
			expected:    []NestSearchResult{},
			expectError: false,
		},
		{
			name:       "No metadata",
			dependTask: "task1",
			parameters: map[string]string{"paramA": "valueA"},
			gcsObjects: map[string][]*storage.ObjectAttrs{
				"test-bucket": {
					{
						Name:     "file1.txt",
						Metadata: nil,
					},
				},
			},
			expected:    []NestSearchResult{},
			expectError: false,
		},
		{
			name:       "No matched metadata",
			dependTask: "task1",
			parameters: map[string]string{"paramA": "wrongValue"},
			gcsObjects: map[string][]*storage.ObjectAttrs{
				"test-bucket": {
					{
						Name: "file1.txt",
						Metadata: map[string]string{
							"required_task_outputs": `{"task1": "gs://output-bucket/task1-result"}`,
						},
					},
				},
				"output-bucket": {
					{
						Name: "task1-result",
						Metadata: map[string]string{
							"paramA": "valueA",
						},
					},
				},
			},
			expected:    []NestSearchResult{},
			expectError: false,
		},
		{
			name:       "invalid GCS path",
			dependTask: "task1",
			parameters: map[string]string{"paramA": "valueA"},
			gcsObjects: map[string][]*storage.ObjectAttrs{
				"test-bucket": {
					{
						Name: "file1.txt",
						Metadata: map[string]string{
							"required_task_outputs": `{"task1": "invalid-path"}`,
						},
					},
				},
			},
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockGCSClient{objects: tt.gcsObjects}
			handler := NewNestSearchHandler(mockClient, "test-bucket", tt.dependTask, tt.parameters)

			results, err := handler.Do()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, *results)
			}
		})
	}
}
