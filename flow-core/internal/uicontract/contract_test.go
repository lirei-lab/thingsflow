package uicontract

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestLoadStableContract(t *testing.T) {
	contract, err := Load("../../testdata/ui_contract/stable.json")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if contract.UIImage != "thingsboard/tb-web-ui:4.3.1.1" {
		t.Fatalf("UIImage = %q, want %q", contract.UIImage, "thingsboard/tb-web-ui:4.3.1.1")
	}
	if got, want := len(contract.Entries), 40; got < want {
		t.Fatalf("stable contract has %d entries, want at least %d", got, want)
	}
	if contract.Entries[0].ID == "" {
		t.Fatal("first entry ID is empty")
	}
}

func TestLoadContractRejectsEmptyEntries(t *testing.T) {
	path := t.TempDir() + "/contract.json"
	if err := os.WriteFile(path, []byte(`{"uiImage":"thingsboard/tb-web-ui:4.3.1.1","entries":[]}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want entries is required")
	}
	if !strings.Contains(err.Error(), "entries is required") {
		t.Fatalf("Load() error = %q, want entries is required", err)
	}
}

func TestValidateJSONShapeObject(t *testing.T) {
	entry := Entry{ID: "sample", ExpectJSON: "object", RequiredKeys: []string{"token"}}
	var body any
	if err := json.Unmarshal([]byte(`{"token":"abc"}`), &body); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if err := ValidateBody(entry, body); err != nil {
		t.Fatalf("ValidateBody() error = %v", err)
	}
}

func TestValidateJSONShapeArray(t *testing.T) {
	entry := Entry{ID: "sample", ExpectJSON: "array"}
	var body any
	if err := json.Unmarshal([]byte(`[{"id":1}]`), &body); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if err := ValidateBody(entry, body); err != nil {
		t.Fatalf("ValidateBody() error = %v", err)
	}
}

func TestValidateJSONShapeNumber(t *testing.T) {
	entry := Entry{ID: "sample", ExpectJSON: "number"}
	var body any
	if err := json.Unmarshal([]byte(`123`), &body); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if err := ValidateBody(entry, body); err != nil {
		t.Fatalf("ValidateBody() error = %v", err)
	}
}

func TestValidateJSONShapeString(t *testing.T) {
	entry := Entry{ID: "sample", ExpectJSON: "string"}
	var body any
	if err := json.Unmarshal([]byte(`"ready"`), &body); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if err := ValidateBody(entry, body); err != nil {
		t.Fatalf("ValidateBody() error = %v", err)
	}
}

func TestValidateJSONShapeMissingKey(t *testing.T) {
	entry := Entry{ID: "sample", ExpectJSON: "object", RequiredKeys: []string{"token"}}
	var body any
	if err := json.Unmarshal([]byte(`{}`), &body); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	err := ValidateBody(entry, body)
	if err == nil {
		t.Fatal("ValidateBody() error = nil, want missing key error")
	}
	if !strings.Contains(err.Error(), "missing required key token") {
		t.Fatalf("ValidateBody() error = %q, want missing required key token", err)
	}
}
