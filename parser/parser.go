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
		gokart_task_name_flag, gokart_output_path_flag := false, false
		for k, _ := range vertex {
			if k == "__gokart_task_name" {
				gokart_task_name_flag = true
			}
			if k == "__gokart_output_path" {
				gokart_output_path_flag = true
			}
		}
		if gokart_task_name_flag && gokart_output_path_flag {
			gokart_task_name, gokart_output_path := "", ""
			for k, v := range vertex {
				switch sv := v.(type) {
				case string:
					if k == "__gokart_task_name" {
						gokart_task_name = sv
					} else if k == "__gokart_output_path" {
						gokart_output_path = sv
					}
				}
			}
			*gokarts = append(*gokarts, Gokart{TaskName: gokart_task_name, OutputPath: gokart_output_path})
		}
		for _, v := range vertex {
			p.traverse(v, gokarts)
		}
	case []interface{}:
		for _, v := range vertex {
			p.traverse(v, gokarts)
		}
	default:
	}
}
