package query

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/iamhimansu/csvquery/pkg/csvquery/types"
)

func ParseCondition(data []byte) (*types.Condition, error) {
	if len(data) == 0 || string(data) == "{}" || string(data) == "[]" {
		return nil, nil
	}
	var simpleMap map[string]interface{}
	if err := json.Unmarshal(data, &simpleMap); err == nil && len(simpleMap) > 0 {
		_, hasOp := simpleMap["operator"]
		if !hasOp {
			root := &types.Condition{
				Operator: "AND",
				Children: make([]types.Condition, 0, len(simpleMap)),
			}
			for col, val := range simpleMap {
				valStr := fmt.Sprintf("%v", val)
				root.Children = append(root.Children, types.Condition{
					Operator: types.OpEq,
					Column:   strings.ToLower(col),
					Value:    valStr,
				})
			}
			ResolveTargets(root)
			return root, nil
		}
	}

	var complexCond types.Condition
	if err := json.Unmarshal(data, &complexCond); err == nil {
		if complexCond.Operator != "" {
			ResolveTargets(&complexCond)
			return &complexCond, nil
		}
	}
	return nil, fmt.Errorf("invalid where format")
}

func ResolveTargets(c *types.Condition) {
	if c.Value != nil {
		c.ResolvedTarget = fmt.Sprintf("%v", c.Value)
	}
	for i := range c.Children {
		ResolveTargets(&c.Children[i])
	}
}

func Evaluate(c *types.Condition, row map[string]string) bool {
	switch c.Operator {
	case "AND":
		for _, child := range c.Children {
			if !Evaluate(&child, row) {
				return false
			}
		}
		return true
	case "OR":
		for _, child := range c.Children {
			if Evaluate(&child, row) {
				return true
			}
		}
		return false
	}

	val, exists := row[c.Column]
	switch c.Operator {
	case types.OpIsNull:
		return !exists || val == "" || val == "NULL"
	case types.OpIsNotNull:
		return exists && val != "" && val != "NULL"
	}

	if !exists {
		return false
	}

	target := c.ResolvedTarget
	switch c.Operator {
	case types.OpEq:
		return val == target
	case types.OpNeq:
		return val != target
	case types.OpGt:
		return val > target
	case types.OpLt:
		return val < target
	case types.OpGte:
		return val >= target
	case types.OpLte:
		return val <= target
	case types.OpLike:
		return strings.Contains(strings.ToLower(val), strings.ToLower(target))
	}

	return false
}

func ExtractBestIndexKey(c *types.Condition) (string, string, bool) {
	conds := ExtractIndexConditions(c)
	for k, v := range conds {
		return k, v, true
	}
	return "", "", false
}

func ExtractIndexConditions(c *types.Condition) map[string]string {
	res := make(map[string]string)
	if c.Operator == "AND" {
		for _, child := range c.Children {
			if child.Operator == types.OpEq {
				res[child.Column] = fmt.Sprintf("%v", child.Value)
			}
		}
	} else if c.Operator == types.OpEq {
		res[c.Column] = fmt.Sprintf("%v", c.Value)
	}
	return res
}
