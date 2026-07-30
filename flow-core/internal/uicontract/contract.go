package uicontract

import (
	"encoding/json"
	"fmt"
	"os"
)

type Contract struct {
	UIImage string  `json:"uiImage"`
	Entries []Entry `json:"entries"`
}

type Entry struct {
	ID           string         `json:"id"`
	Area         string         `json:"area"`
	Class        string         `json:"class"`
	Method       string         `json:"method"`
	Path         string         `json:"path"`
	Auth         string         `json:"auth"`
	Body         map[string]any `json:"body"`
	ExpectStatus int            `json:"expectStatus"`
	ExpectJSON   string         `json:"expectJson"`
	RequiredKeys []string       `json:"requiredKeys"`
}

func Load(path string) (*Contract, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var contract Contract
	if err := json.Unmarshal(data, &contract); err != nil {
		return nil, err
	}

	if contract.UIImage == "" {
		return nil, fmt.Errorf("uiImage is required")
	}
	if len(contract.Entries) == 0 {
		return nil, fmt.Errorf("entries is required")
	}
	for i, entry := range contract.Entries {
		if entry.ID == "" {
			return nil, fmt.Errorf("entry %d missing id", i)
		}
		if entry.Method == "" {
			return nil, fmt.Errorf("entry %s missing method", entry.ID)
		}
		if entry.Path == "" {
			return nil, fmt.Errorf("entry %s missing path", entry.ID)
		}
		if entry.ExpectStatus == 0 {
			return nil, fmt.Errorf("entry %s missing expectStatus", entry.ID)
		}
	}

	return &contract, nil
}

func ValidateBody(entry Entry, body any) error {
	switch entry.ExpectJSON {
	case "", "any":
		return nil
	case "object":
		object, ok := body.(map[string]any)
		if !ok {
			return fmt.Errorf("entry %s expected JSON object", entry.ID)
		}
		for _, key := range entry.RequiredKeys {
			if _, ok := object[key]; !ok {
				return fmt.Errorf("entry %s missing required key %s", entry.ID, key)
			}
		}
		return nil
	case "array":
		if _, ok := body.([]any); !ok {
			return fmt.Errorf("entry %s expected JSON array", entry.ID)
		}
		return nil
	case "number":
		if _, ok := body.(float64); !ok {
			return fmt.Errorf("entry %s expected JSON number", entry.ID)
		}
		return nil
	case "string":
		if _, ok := body.(string); !ok {
			return fmt.Errorf("entry %s expected JSON string", entry.ID)
		}
		return nil
	default:
		return fmt.Errorf("entry %s unknown expectJson %q", entry.ID, entry.ExpectJSON)
	}
}
