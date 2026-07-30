package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"flow-core/internal/uicontract"
)

func TestRunEntryUsesTenantToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/login":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]string{"token": "abc"}); err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
		case "/api/page-data":
			if got := r.Header.Get("X-Authorization"); got != "Bearer abc" {
				t.Fatalf("X-Authorization = %q, want %q", got, "Bearer abc")
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{"data": []any{}, "hasNext": false}); err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	runner := Runner{BaseURL: server.URL, HTTP: server.Client()}
	if err := runner.Login("tenant@thingsboard.org", "tenant"); err != nil {
		t.Fatalf("Login() error = %v", err)
	}

	result := runner.RunEntry(uicontract.Entry{
		ID:           "page-data",
		Area:         "devices",
		Class:        "PageData",
		Method:       http.MethodGet,
		Path:         "/api/page-data",
		Auth:         "tenant",
		ExpectStatus: http.StatusOK,
		ExpectJSON:   "object",
		RequiredKeys: []string{"data", "hasNext"},
	})

	if !result.OK {
		t.Fatalf("RunEntry() OK = false, Status = %d, Error = %q", result.Status, result.Error)
	}
}

func TestRunEntryReportsStatusMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer server.Close()

	runner := Runner{BaseURL: server.URL, HTTP: server.Client()}
	result := runner.RunEntry(uicontract.Entry{
		ID:           "missing",
		Area:         "devices",
		Class:        "PageData",
		Method:       http.MethodGet,
		Path:         "/api/missing",
		ExpectStatus: http.StatusOK,
	})

	if result.OK {
		t.Fatal("RunEntry() OK = true, want false")
	}
	if result.Status != http.StatusNotFound {
		t.Fatalf("RunEntry() Status = %d, want %d", result.Status, http.StatusNotFound)
	}
}

func TestRunEntryRejectsWrongJSONContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"data":[]}`)); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}))
	defer server.Close()

	runner := Runner{BaseURL: server.URL, HTTP: server.Client()}
	result := runner.RunEntry(uicontract.Entry{
		ID:           "wrong-content-type",
		Method:       http.MethodGet,
		Path:         "/api/page-data",
		ExpectStatus: http.StatusOK,
		ExpectJSON:   "object",
	})

	if result.OK {
		t.Fatal("RunEntry() OK = true, want false")
	}
	if !strings.Contains(result.Error, "content-type text/plain, want application/json") {
		t.Fatalf("RunEntry() Error = %q, want content-type mismatch", result.Error)
	}
}

func TestRunEntryRejectsMissingJSONContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	runner := Runner{BaseURL: server.URL, HTTP: server.Client()}
	result := runner.RunEntry(uicontract.Entry{
		ID:           "missing-content-type",
		Method:       http.MethodGet,
		Path:         "/api/page-data",
		ExpectStatus: http.StatusOK,
		ExpectJSON:   "any",
	})

	if result.OK {
		t.Fatal("RunEntry() OK = true, want false")
	}
	if !strings.Contains(result.Error, "missing content-type") {
		t.Fatalf("RunEntry() Error = %q, want missing content-type", result.Error)
	}
}

func TestRunEntryRejectsInvalidJSONForAnyExpectation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`not-json`)); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}))
	defer server.Close()

	runner := Runner{BaseURL: server.URL, HTTP: server.Client()}
	result := runner.RunEntry(uicontract.Entry{
		ID:           "invalid-json",
		Method:       http.MethodGet,
		Path:         "/api/primitive",
		ExpectStatus: http.StatusOK,
		ExpectJSON:   "any",
	})

	if result.OK {
		t.Fatal("RunEntry() OK = true, want false")
	}
	if result.Error == "" {
		t.Fatal("RunEntry() Error is empty, want JSON decode error")
	}
}

func TestRunEntryAcceptsProblemJSONContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"type":"about:blank"}`)); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}))
	defer server.Close()

	runner := Runner{BaseURL: server.URL, HTTP: server.Client()}
	result := runner.RunEntry(uicontract.Entry{
		ID:           "problem-json",
		Method:       http.MethodGet,
		Path:         "/api/problem",
		ExpectStatus: http.StatusOK,
		ExpectJSON:   "object",
		RequiredKeys: []string{"type"},
	})

	if !result.OK {
		t.Fatalf("RunEntry() OK = false, Status = %d, Error = %q", result.Status, result.Error)
	}
}
