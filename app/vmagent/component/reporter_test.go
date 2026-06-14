package component

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type testSnapshotProvider struct {
	snapshot Snapshot
}

func (p *testSnapshotProvider) Snapshot() Snapshot {
	return p.snapshot
}

func TestReporterRegisterAndHeartbeat(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var payloads []map[string]interface{}
	var componentIDs []string
	var apiKeys []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("cannot decode payload: %s", err)
		}
		mu.Lock()
		paths = append(paths, r.URL.Path)
		payloads = append(payloads, payload)
		componentIDs = append(componentIDs, r.Header.Get("X-Monitor-Component-ID"))
		apiKeys = append(apiKeys, r.Header.Get("X-Monitor-API-Key"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := NewReporter(Config{
		Enabled:              true,
		RegisterURL:          srv.URL + "/monitor/components/register",
		HeartbeatURL:         srv.URL + "/monitor/components/heartbeat",
		APIKey:               "secret",
		ComponentID:          "vmagent-bj-01",
		Name:                 "vmagent bj",
		Endpoint:             "http://10.0.0.1:8429",
		MetricsEndpoint:      "http://10.0.0.1:8429/metrics",
		Zone:                 "bj",
		CurrentConfigVersion: "cfg-1",
		HeartbeatInterval:    time.Second,
		Workload: map[string]interface{}{
			"http_listen_addrs": []string{":8429"},
		},
	}, &testSnapshotProvider{
		snapshot: Snapshot{
			Workload: map[string]interface{}{
				"target_count": float64(42),
			},
		},
	})

	if err := r.Register(); err != nil {
		t.Fatalf("unexpected register error: %s", err)
	}
	if err := r.Heartbeat(); err != nil {
		t.Fatalf("unexpected heartbeat error: %s", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := paths, []string{"/monitor/components/register", "/monitor/components/heartbeat"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected paths; got %v; want %v", got, want)
	}
	if got := componentIDs; got[0] != "vmagent-bj-01" || got[1] != "vmagent-bj-01" {
		t.Fatalf("unexpected component id headers: %v", got)
	}
	if got := apiKeys; got[0] != "secret" || got[1] != "secret" {
		t.Fatalf("unexpected api key headers: %v", got)
	}
	registerPayload := payloads[0]
	if got, want := registerPayload["component_type"], "vmagent"; got != want {
		t.Fatalf("unexpected register component_type; got %v; want %v", got, want)
	}
	if got, want := registerPayload["component_id"], "vmagent-bj-01"; got != want {
		t.Fatalf("unexpected register component_id; got %v; want %v", got, want)
	}
	heartbeatPayload := payloads[1]
	if got, want := heartbeatPayload["status"], "online"; got != want {
		t.Fatalf("unexpected heartbeat status; got %v; want %v", got, want)
	}
	workload := heartbeatPayload["workload"].(map[string]interface{})
	if got, want := workload["target_count"], float64(42); got != want {
		t.Fatalf("unexpected workload target_count; got %v; want %v", got, want)
	}
}

func TestReporterDisabledWithoutURLs(t *testing.T) {
	r := NewReporter(Config{
		Enabled:     true,
		ComponentID: "vmagent",
	}, nil)
	if r.Enabled() {
		t.Fatalf("reporter must be disabled when both register and heartbeat urls are empty")
	}
}

func TestHeartbeatPayloadDegradedOnLastError(t *testing.T) {
	r := NewReporter(Config{
		Enabled:           true,
		HeartbeatURL:      "http://monitor-center.invalid/monitor/components/heartbeat",
		ComponentID:       "vmagent",
		HeartbeatInterval: time.Second,
	}, &testSnapshotProvider{
		snapshot: Snapshot{LastError: "remote write pending"},
	})
	payload := r.heartbeatPayload()
	if got, want := payload["status"], "degraded"; got != want {
		t.Fatalf("unexpected status; got %v; want %v", got, want)
	}
	if got, want := payload["last_error"], "remote write pending"; got != want {
		t.Fatalf("unexpected last_error; got %v; want %v", got, want)
	}
}

func TestNewReporterFromFlagsUsesExplicitComponentEndpoints(t *testing.T) {
	oldEnabled := *enabled
	oldRegisterURL := *registerURL
	oldHeartbeatURL := *heartbeatURL
	oldComponentID := *componentID
	oldComponentName := *componentName
	oldEndpoint := *endpoint
	oldMetricsEndpoint := *metricsEndpoint
	defer func() {
		*enabled = oldEnabled
		*registerURL = oldRegisterURL
		*heartbeatURL = oldHeartbeatURL
		*componentID = oldComponentID
		*componentName = oldComponentName
		*endpoint = oldEndpoint
		*metricsEndpoint = oldMetricsEndpoint
	}()

	*enabled = true
	*registerURL = "http://monitor-center.invalid/monitor/components/register"
	*heartbeatURL = "http://monitor-center.invalid/monitor/components/heartbeat"
	*componentID = "vmagent-shard-0"
	*componentName = "vmagent shard 0"
	*endpoint = "http://10.107.251.101:8429"
	*metricsEndpoint = "http://10.107.251.101:8429/metrics"

	r := NewReporterFromFlags([]string{":8429"})
	registerPayload := r.registerPayload()
	if got, want := registerPayload["endpoint"], "http://10.107.251.101:8429"; got != want {
		t.Fatalf("unexpected register endpoint; got %v; want %v", got, want)
	}
	if got, want := registerPayload["metrics_endpoint"], "http://10.107.251.101:8429/metrics"; got != want {
		t.Fatalf("unexpected register metrics endpoint; got %v; want %v", got, want)
	}
	heartbeatPayload := r.heartbeatPayload()
	if got, want := heartbeatPayload["endpoint"], "http://10.107.251.101:8429"; got != want {
		t.Fatalf("unexpected heartbeat endpoint; got %v; want %v", got, want)
	}
	if got, want := heartbeatPayload["metrics_endpoint"], "http://10.107.251.101:8429/metrics"; got != want {
		t.Fatalf("unexpected heartbeat metrics endpoint; got %v; want %v", got, want)
	}
}

func TestFirstHTTPListenEndpoint(t *testing.T) {
	f := func(addrs []string, path, want string) {
		t.Helper()
		if got := firstHTTPListenEndpoint(addrs, path); got != want {
			t.Fatalf("unexpected endpoint; got %q; want %q", got, want)
		}
	}
	f([]string{":8429"}, "/metrics", "http://127.0.0.1:8429/metrics")
	f([]string{"10.0.0.1:8429"}, "", "http://10.0.0.1:8429")
	f([]string{"https://vmagent.example:8429/"}, "/metrics", "https://vmagent.example:8429/metrics")
}
