package executor

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/SheykoWk/workflow-engine/internal/app/ports"
)

// Matches {{steps.<n>.output.<dotted.path>}} — path is resolved inside that step's output JSON.
var stepOutputPlaceholderRE = regexp.MustCompile(`\{\{steps\.(\d+)\.output\.([^}]+)\}\}`)

// interpolateStepConfig replaces placeholders using outputs from steps with step_index < currentStepIndex.
func interpolateStepConfig(config []byte, rows []ports.StepIndexOutput, currentStepIndex int) ([]byte, error) {
	if len(config) == 0 {
		return config, nil
	}

	byIndex := make(map[int][]byte)
	for _, row := range rows {
		if row.StepIndex >= currentStepIndex {
			continue
		}
		out := row.Output
		if len(out) == 0 {
			out = []byte("{}")
		}
		byIndex[row.StepIndex] = out
	}

	parsed := make(map[int]any)
	getRoot := func(stepIdx int) (any, error) {
		if v, ok := parsed[stepIdx]; ok {
			return v, nil
		}
		raw, ok := byIndex[stepIdx]
		if !ok {
			parsed[stepIdx] = map[string]any{}
			return parsed[stepIdx], nil
		}
		var root any
		if err := json.Unmarshal(raw, &root); err != nil {
			return nil, fmt.Errorf("step %d output: %w", stepIdx, err)
		}
		parsed[stepIdx] = root
		return root, nil
	}

	s := string(config)
	for _, m := range stepOutputPlaceholderRE.FindAllStringSubmatch(s, -1) {
		if len(m) < 3 {
			continue
		}
		stepIdx, err := strconv.Atoi(m[1])
		if err != nil || stepIdx >= currentStepIndex {
			continue
		}
		if _, err := getRoot(stepIdx); err != nil {
			return nil, err
		}
	}

	out := stepOutputPlaceholderRE.ReplaceAllStringFunc(s, func(match string) string {
		parts := stepOutputPlaceholderRE.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		stepIdx, err := strconv.Atoi(parts[1])
		if err != nil || stepIdx >= currentStepIndex {
			return ""
		}
		path := strings.Trim(parts[2], ".")
		if path == "" {
			return ""
		}
		root, err := getRoot(stepIdx)
		if err != nil {
			return ""
		}
		segs := strings.Split(path, ".")
		val, ok := jsonLookup(root, segs)
		if !ok {
			return ""
		}
		return replaceFragmentForJSONString(val)
	})

	if !json.Valid([]byte(out)) {
		return nil, fmt.Errorf("interpolate: result is not valid JSON")
	}
	return []byte(out), nil
}

func jsonLookup(root any, path []string) (any, bool) {
	cur := root
	for _, p := range path {
		if cur == nil {
			return nil, false
		}
		switch n := cur.(type) {
		case map[string]any:
			v, ok := n[p]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			i, err := strconv.Atoi(p)
			if err != nil || i < 0 || i >= len(n) {
				return nil, false
			}
			cur = n[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

// replaceFragmentForJSONString returns text safe to splice into a JSON string value (escapes quotes etc.).
func replaceFragmentForJSONString(v any) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	s := string(b)
	if len(s) >= 2 && s[0] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
