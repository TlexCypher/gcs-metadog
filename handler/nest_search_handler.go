package handler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"gcs-metadog/parser"
	"github.com/samber/lo"
	"google.golang.org/api/iterator"
)

type NestSearchHandler struct {
	gcsClient  RealGCSClient
	bucketName string
	dependTask string
	parameters map[string]string
}

func NewNestSearchHandler(gcsClient RealGCSClient, bucketName string, dependTask string, parameters map[string]string) *NestSearchHandler {
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

type Dependency struct {
	OriginalPath string
	Dep          parser.Gokart
}

func (h *NestSearchHandler) Do() ([]NestSearchResult, error) {
	ctx := context.Background()

	itr := h.gcsClient.Objects(ctx, h.bucketName)
	deps := make([]Dependency, 0)
	for {
		objAttrs, err := itr.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			slog.Error("failed to iterate objects", slog.String("error", err.Error()))
			return nil, err
		}
		requiredTaskPathsJSONStr, exists := objAttrs.Metadata["__required_task_outputs"]
		if !exists {
			continue
		}
		p := parser.NewParser(requiredTaskPathsJSONStr)
		gs, err := p.Parse()
		if err != nil {
			return nil, err
		}
		for _, g := range gs {
			deps = append(deps, Dependency{OriginalPath: "gs://" + objAttrs.Bucket + "/" + objAttrs.Name, Dep: g})
		}
	}
	/* filter by task name.*/
	filtered := lo.Filter(deps, func(dep Dependency, index int) bool {
		return dep.Dep.TaskName == h.dependTask
	})
	/*filter by parameters*/
	filtered, err := h.filterByParameters(ctx, filtered)
	if err != nil {
		return nil, err
	}
	return lo.Map(filtered, func(d Dependency, index int) NestSearchResult {
		return NestSearchResult{
			ObjectPath: d.OriginalPath,
		}
	}), nil
}

func (h *NestSearchHandler) filterByParameters(ctx context.Context, filtered []Dependency) ([]Dependency, error) {
	deps := make([]Dependency, 0)
	partsCount := 2

	for _, d := range filtered {
		noPrefixPath := strings.TrimPrefix(d.Dep.OutputPath, "gs://")
		parts := strings.SplitN(noPrefixPath, "/", partsCount)
		objAttrs, err := h.gcsClient.client.Bucket(parts[0]).Object(parts[1]).Attrs(ctx)
		if err != nil {
			return nil, err
		}
		allConditionMatched := true
		for paramName, paramValue := range h.parameters {
			metadataValue, exists := objAttrs.Metadata[paramName]
			if !exists {
				allConditionMatched = false
			} else if metadataValue != paramValue {
				allConditionMatched = false
			}
		}
		if allConditionMatched {
			deps = append(deps, d)
		}
	}
	return deps, nil
}
