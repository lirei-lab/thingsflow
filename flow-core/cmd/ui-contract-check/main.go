package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"strings"
	"time"

	"flow-core/internal/uicontract"
)

type Runner struct {
	BaseURL string
	HTTP    *http.Client
	Token   string
}

type Result struct {
	ID     string `json:"id"`
	Area   string `json:"area"`
	Class  string `json:"class"`
	OK     bool   `json:"ok"`
	Status int    `json:"status"`
	Error  string `json:"error,omitempty"`
}

func (r *Runner) Login(username, password string) error {
	body, err := json.Marshal(map[string]string{
		"username": username,
		"password": password,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, r.urlFor("/api/auth/login"), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("login returned status %d", resp.StatusCode)
	}

	var login struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&login); err != nil {
		return err
	}
	if login.Token == "" {
		return fmt.Errorf("login response missing token")
	}
	r.Token = login.Token
	return nil
}

func (r *Runner) RunEntry(entry uicontract.Entry) Result {
	result := Result{
		ID:    entry.ID,
		Area:  entry.Area,
		Class: entry.Class,
	}

	var body io.Reader
	if entry.Body != nil {
		data, err := json.Marshal(entry.Body)
		if err != nil {
			result.Error = err.Error()
			return result
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(entry.Method, r.urlFor(entry.Path), body)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if entry.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if entry.Auth == "tenant" && r.Token != "" {
		req.Header.Set("X-Authorization", "Bearer "+r.Token)
	}

	resp, err := r.client().Do(req)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer resp.Body.Close()

	result.Status = resp.StatusCode
	if resp.StatusCode != entry.ExpectStatus {
		result.Error = fmt.Sprintf("expected status %d, got %d", entry.ExpectStatus, resp.StatusCode)
		return result
	}

	if entry.ExpectJSON != "" {
		if err := validateJSONContentType(resp.Header.Get("Content-Type")); err != nil {
			result.Error = err.Error()
			return result
		}
	}

	if entry.ExpectJSON != "" {
		var responseBody any
		if err := json.NewDecoder(resp.Body).Decode(&responseBody); err != nil {
			result.Error = err.Error()
			return result
		}
		if err := uicontract.ValidateBody(entry, responseBody); err != nil {
			result.Error = err.Error()
			return result
		}
	}

	result.OK = true
	return result
}

func validateJSONContentType(contentType string) error {
	if contentType == "" {
		return fmt.Errorf("missing content-type")
	}

	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return fmt.Errorf("content-type %s, want application/json", contentType)
	}
	if mediaType == "application/json" || strings.HasPrefix(mediaType, "application/") && strings.HasSuffix(mediaType, "+json") {
		return nil
	}
	return fmt.Errorf("content-type %s, want application/json", mediaType)
}

func (r *Runner) client() *http.Client {
	if r.HTTP != nil {
		return r.HTTP
	}
	return http.DefaultClient
}

func (r *Runner) urlFor(path string) string {
	return strings.TrimRight(r.BaseURL, "/") + "/" + strings.TrimLeft(path, "/")
}

func main() {
	contractPath := flag.String("contract", "testdata/ui_contract/stable.json", "path to UI contract JSON")
	baseURL := flag.String("base-url", "http://localhost:8082", "base URL for the target HTTP server")
	username := flag.String("username", "tenant@thingsboard.org", "tenant username")
	password := flag.String("password", "tenant", "tenant password")
	jsonOutput := flag.Bool("json", false, "print JSON report")
	flag.Parse()

	contract, err := uicontract.Load(*contractPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load contract: %v\n", err)
		os.Exit(2)
	}

	runner := Runner{
		BaseURL: *baseURL,
		HTTP:    &http.Client{Timeout: 30 * time.Second},
	}
	if err := runner.Login(*username, *password); err != nil {
		fmt.Fprintf(os.Stderr, "login: %v\n", err)
		os.Exit(2)
	}

	results := make([]Result, 0, len(contract.Entries))
	failures := 0
	for _, entry := range contract.Entries {
		result := runner.RunEntry(entry)
		if !result.OK {
			failures++
		}
		results = append(results, result)
	}

	if *jsonOutput {
		report := struct {
			OK      bool     `json:"ok"`
			Results []Result `json:"results"`
		}{
			OK:      failures == 0,
			Results: results,
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			os.Exit(2)
		}
	} else {
		for _, result := range results {
			status := "OK"
			if !result.OK {
				status = "FAIL"
			}
			if result.Error == "" {
				fmt.Printf("%s %s status=%d\n", status, result.ID, result.Status)
			} else {
				fmt.Printf("%s %s status=%d error=%s\n", status, result.ID, result.Status, result.Error)
			}
		}
		fmt.Printf("checked %d entries, %d failures\n", len(results), failures)
	}

	if failures > 0 {
		os.Exit(1)
	}
}
