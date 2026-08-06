package main

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestTwinModelOpenAPIOperationsHaveExactMuxPatterns(t *testing.T) {
	operations := parseTwinModelOpenAPIOperations(t, "../docs/api/openapi.yaml")
	want := map[string]struct{}{
		"GET /api/twin-models":                         {},
		"POST /api/twin-models":                        {},
		"GET /api/twin-models/{modelId}/{version}":     {},
		"DELETE /api/twin-models/{modelId}/{version}":  {},
		"PUT /api/twins/{entityType}/{entityId}/model": {},
	}
	if len(operations) != len(want) {
		t.Fatalf("OpenAPI twin-model operations=%v, want=%v", operations, want)
	}
	for operation := range want {
		if _, ok := operations[operation]; !ok {
			t.Fatalf("OpenAPI lacks required operation %s", operation)
		}
	}

	mux := http.NewServeMux()
	registerRoutes(mux, "*")
	for operation := range operations {
		parts := strings.SplitN(operation, " ", 2)
		path := materializeTwinModelPath(parts[1])
		request := httptest.NewRequest(parts[0], path, nil)
		_, pattern := mux.Handler(request)
		if pattern != operation {
			t.Fatalf("%s matched mux pattern %q", operation, pattern)
		}
	}
}

func TestTwinModelRoutesAreDenyByDefault(t *testing.T) {
	mux := http.NewServeMux()
	registerRoutes(mux, "*")
	gated := authGate(mux, "*")
	for _, operation := range []string{
		"GET /api/twin-models",
		"POST /api/twin-models",
		"GET /api/twin-models/energy_meter/1.0.0",
		"DELETE /api/twin-models/energy_meter/1.0.0",
		"PUT /api/twins/DEVICE/11111111-1111-1111-1111-111111111111/model",
	} {
		parts := strings.SplitN(operation, " ", 2)
		response := httptest.NewRecorder()
		gated.ServeHTTP(response, httptest.NewRequest(parts[0], parts[1], nil))
		if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), `"errorCode"`) {
			t.Fatalf("unauthenticated %s status=%d body=%s", operation, response.Code, response.Body.String())
		}
	}
}

func parseTwinModelOpenAPIOperations(t *testing.T, path string) map[string]struct{} {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open OpenAPI: %v", err)
	}
	defer file.Close()

	operations := map[string]struct{}{}
	inPaths := false
	currentPath := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if line == "paths:" {
			inPaths = true
			continue
		}
		if inPaths && line == "components:" {
			break
		}
		if !inPaths {
			continue
		}
		if strings.HasPrefix(line, "  /") && strings.HasSuffix(trimmed, ":") {
			currentPath = strings.TrimSuffix(trimmed, ":")
			continue
		}
		if currentPath == "" || !(strings.HasPrefix(currentPath, "/api/twin-models") || currentPath == "/api/twins/{entityType}/{entityId}/model") {
			continue
		}
		if strings.HasPrefix(line, "    ") && !strings.HasPrefix(line, "      ") {
			method := strings.TrimSuffix(trimmed, ":")
			switch method {
			case "get", "post", "put", "delete", "patch":
				operations[strings.ToUpper(method)+" "+currentPath] = struct{}{}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan OpenAPI: %v", err)
	}
	return operations
}

func materializeTwinModelPath(path string) string {
	return strings.NewReplacer(
		"{modelId}", "energy_meter",
		"{version}", "1.0.0",
		"{entityType}", "DEVICE",
		"{entityId}", "11111111-1111-1111-1111-111111111111",
	).Replace(path)
}
