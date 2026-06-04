package component

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestReporterSyncFetchesAndAppliesDesiredConfig(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var componentIDs []string
	var apiKeys []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.String())
		componentIDs = append(componentIDs, r.Header.Get("X-Monitor-Component-ID"))
		apiKeys = append(apiKeys, r.Header.Get("X-Monitor-API-Key"))
		mu.Unlock()

		switch r.URL.Path {
		case "/monitor/components/register":
			writeAPIResponse(t, w, map[string]interface{}{
				"desired_config_version": "cfg-new",
			})
		case "/monitor/components/heartbeat":
			writeAPIResponse(t, w, heartbeatResponse{
				DesiredConfigVersion:    "cfg-new",
				CurrentConfigVersion:    "cfg-old",
				ConfigSyncStatus:        "out_of_sync",
				NeedResync:              true,
				HeartbeatIntervalSecond: 60,
			})
		case "/monitor/components/vmalert-main/config":
			if got, want := r.URL.Query().Get("version"), "cfg-new"; got != want {
				t.Fatalf("unexpected config version query; got %q; want %q", got, want)
			}
			writeAPIResponse(t, w, configResponse{
				ComponentID:   "vmalert-main",
				ComponentType: "vmalert",
				Version:       "cfg-new",
				ContentHash:   "sha256:test",
				Content:       "groups:\n- name: test\n",
			})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	var appliedVersion, appliedContent string
	r := NewReporter(Config{
		Enabled:              true,
		RegisterURL:          srv.URL + "/monitor/components/register",
		HeartbeatURL:         srv.URL + "/monitor/components/heartbeat",
		ConfigURL:            srv.URL + "/monitor/components/vmalert-main/config",
		APIKey:               "secret",
		ComponentID:          "vmalert-main",
		Name:                 "vmalert main",
		CurrentConfigVersion: "cfg-old",
		HeartbeatInterval:    time.Second,
		ApplyConfig: func(version, content string) error {
			appliedVersion = version
			appliedContent = content
			return nil
		},
	}, nil)

	if err := r.Register(); err != nil {
		t.Fatalf("unexpected register error: %s", err)
	}
	if err := r.SyncOnce(""); err != nil {
		t.Fatalf("unexpected sync error: %s", err)
	}
	if got, want := appliedVersion, "cfg-new"; got != want {
		t.Fatalf("unexpected applied version; got %q; want %q", got, want)
	}
	if got, want := appliedContent, "groups:\n- name: test\n"; got != want {
		t.Fatalf("unexpected applied content; got %q; want %q", got, want)
	}
	if got, want := r.CurrentConfigVersion(), "cfg-new"; got != want {
		t.Fatalf("unexpected current config version; got %q; want %q", got, want)
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := paths, []string{"/monitor/components/register", "/monitor/components/heartbeat", "/monitor/components/vmalert-main/config?version=cfg-new"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("unexpected paths; got %v; want %v", got, want)
	}
	for i := range componentIDs {
		if componentIDs[i] != "vmalert-main" {
			t.Fatalf("unexpected component id headers: %v", componentIDs)
		}
		if apiKeys[i] != "secret" {
			t.Fatalf("unexpected api key headers: %v", apiKeys)
		}
	}
}

func TestReporterKeepsExplicitEndpoints(t *testing.T) {
	r := NewReporter(Config{
		ComponentID:     "vmalert-main",
		Endpoint:        "http://10.254.25.181:31610",
		MetricsEndpoint: "http://10.254.25.181:31610/metrics",
	}, nil)

	payload := r.registerPayload()
	if got, want := payload["endpoint"], "http://10.254.25.181:31610"; got != want {
		t.Fatalf("unexpected endpoint; got %q; want %q", got, want)
	}
	if got, want := payload["metrics_endpoint"], "http://10.254.25.181:31610/metrics"; got != want {
		t.Fatalf("unexpected metrics endpoint; got %q; want %q", got, want)
	}
}

func TestApplyManagedConfigValidatesAndReplacesFile(t *testing.T) {
	dir := t.TempDir()
	managedFile := filepath.Join(dir, "rules.yml")
	versionFile := filepath.Join(dir, "rules.version")
	if err := os.WriteFile(managedFile, []byte("old"), 0o600); err != nil {
		t.Fatalf("cannot write old config: %s", err)
	}

	var validatedFiles []string
	reloaded := false
	err := ApplyManagedConfig(ApplyOptions{
		ManagedConfigFile: managedFile,
		VersionFile:       versionFile,
		RuleFiles:         []string{managedFile},
		Version:           "cfg-1",
		Content:           "new",
		Validate: func(files []string) error {
			validatedFiles = append([]string(nil), files...)
			content, err := os.ReadFile(files[0])
			if err != nil {
				return err
			}
			if string(content) != "new" {
				t.Fatalf("validator saw unexpected content %q", content)
			}
			return nil
		},
		Reload: func() error {
			reloaded = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected apply error: %s", err)
	}
	if len(validatedFiles) != 1 || validatedFiles[0] == managedFile {
		t.Fatalf("validation must use temporary replacement file; got %v", validatedFiles)
	}
	content, err := os.ReadFile(managedFile)
	if err != nil {
		t.Fatalf("cannot read managed config: %s", err)
	}
	if got, want := string(content), "new"; got != want {
		t.Fatalf("unexpected managed config content; got %q; want %q", got, want)
	}
	version, err := os.ReadFile(versionFile)
	if err != nil {
		t.Fatalf("cannot read version file: %s", err)
	}
	if got, want := string(version), "cfg-1\n"; got != want {
		t.Fatalf("unexpected version file content; got %q; want %q", got, want)
	}
	if !reloaded {
		t.Fatalf("reload callback wasn't called")
	}
}

func writeAPIResponse(t *testing.T, w http.ResponseWriter, data interface{}) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"code":    http.StatusOK,
		"message": "ok",
		"status":  "success",
		"data":    data,
	}); err != nil {
		t.Fatalf("cannot write response: %s", err)
	}
}
