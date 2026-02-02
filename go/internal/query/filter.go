package query

import (
	"encoding/json"
	"fmt"
	"strings"
)

type FilterOp string

const (
	OpEq        FilterOp = "="
	OpNeq       FilterOp = "!="
	OpGt        FilterOp = ">"
	OpLt        FilterOp = "<"
	OpGte       FilterOp = ">="
	OpLte       FilterOp = "<="
	OpLike      FilterOp = "LIKE"
	OpIsNull    FilterOp = "IS NULL"
	OpIsNotNull FilterOp = "IS NOT NULL"
	OpIn        FilterOp = "IN"
)

type Condition struct {
	Operator       FilterOp    `json:"operator"`
	Column         string      `json:"column,omitempty"`
	Value          interface{} `json:"value,omitempty"`
	Children       []Condition `json:"children,omitempty"`
	resolvedTarget string
}

func (c *Condition) resolveTargets() {
	if c.Value != nil {
		c.resolvedTarget = fmt.Sprintf("%v", c.Value)
	}
	for i := range c.Children {
		c.Children[i].resolveTargets()
	}
}

func (c *Condition) Evaluate(row map[string]string) bool {
	switch c.Operator {
	case "AND":
		for _, child := range c.Children {
			if !child.Evaluate(row) {
				return false
			}
		}
		return true
	case "OR":
		for _, child := range c.Children {
			if child.Evaluate(row) {
				return true
			}
		}
		return false
	}

	val, exists := row[c.Column]
	switch c.Operator {
	case OpIsNull:
		return !exists || val == "" || val == "NULL"
	case OpIsNotNull:
		return exists && val != "" && val != "NULL"
	}

	if !exists {
		return false
	}

	target := c.resolvedTarget
	switch c.Operator {
	case OpEq:
		return val == target
	case OpNeq:
		return val != target
	case OpGt:
		return val > target
	case OpLt:
		return val < target
	case OpGte:
		return val >= target
	case OpLte:
		return val <= target
	case OpLike:
		return strings.Contains(strings.ToLower(val), strings.ToLower(target))
	}

	return false
}

func (c *Condition) ExtractBestIndexKey() (string, string, bool) {
	conds := c.ExtractIndexConditions()
	for k, v := range conds {
		return k, v, true
	}
	return "", "", false
}

func (c *Condition) ExtractIndexConditions() map[string]string {
	res := make(map[string]string)
	if c.Operator == "AND" {
		for _, child := range c.Children {
			if child.Operator == OpEq {
				res[child.Column] = fmt.Sprintf("%v", child.Value)
			}
		}
	} else if c.Operator == OpEq {
		res[c.Column] = fmt.Sprintf("%v", c.Value)
	}
	return res
}

func ParseCondition(data []byte) (*Condition, error) {
	if len(data) == 0 || string(data) == "{}" || string(data) == "[]" {
		return nil, nil
	}
	var simpleMap map[string]interface{}
	if err := json.Unmarshal(data, &simpleMap); err == nil && len(simpleMap) > 0 {
		_, hasOp := simpleMap["operator"]
		if !hasOp {
			root := &Condition{
				Operator: "AND",
				Children: make([]Condition, 0, len(simpleMap)),
			}
			for col, val := range simpleMap {
				valStr := fmt.Sprintf("%v", val)
				root.Children = append(root.Children, Condition{
					Operator: OpEq,
					Column:   strings.ToLower(col),
					Value:    valStr,
				})
			}
			root.resolveTargets()
			return root, nil
		}
	}

	var complexCond Condition
	if err := json.Unmarshal(data, &complexCond); err == nil {
		if complexCond.Operator != "" {
			complexCond.resolveTargets()
			return &complexCond, nil
		}
	}
	return nil, fmt.Errorf("invalid where format")
}
