package parser

import (
	"encoding/json"
)

type Parser struct {
	Target string
}

type Gokart struct {
	TaskName   string
	OutputPath string
}

func NewParser(target string) *Parser {
	return &Parser{Target: target}
}

func (p *Parser) Parse() ([]Gokart, error) {
	var b interface{}
	if err := json.Unmarshal([]byte(p.Target), &b); err != nil {
		return nil, err
	}
	gokarts := make([]Gokart, 0)
	p.traverse(b, &gokarts)
	return gokarts, nil
}

func (p *Parser) traverse(b interface{}, gokarts *[]Gokart) {
	switch vertex := b.(type) {
	case map[string]interface{}:
		gokartTaskName, gokartOutputPath := "", ""
		hasTaskName, hasOutputPath := false, false

		for k, v := range vertex {
			if sv, ok := v.(string); ok {
				switch k {
				case "__gokart_task_name":
					gokartTaskName, hasTaskName = sv, true
				case "__gokart_output_path":
					gokartOutputPath, hasOutputPath = sv, true
				}
			}
		}

		if hasTaskName && hasOutputPath {
			*gokarts = append(*gokarts, Gokart{TaskName: gokartTaskName, OutputPath: gokartOutputPath})
		}

		p.traverseChildren(vertex, gokarts)

	case []interface{}:
		p.traverseChildren(vertex, gokarts)
	}
}

func (p *Parser) traverseChildren(data interface{}, gokarts *[]Gokart) {
	switch d := data.(type) {
	case map[string]interface{}:
		for _, v := range d {
			p.traverse(v, gokarts)
		}
	case []interface{}:
		for _, v := range d {
			p.traverse(v, gokarts)
		}
	}
}
