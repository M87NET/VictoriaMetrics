package component

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

const componentType = "vmagent"

var (
	enabled     = flag.Bool("componentReporter.enabled", false, "Whether to enable reporting vmagent component registration and heartbeat to monitor center")
	registerURL = flag.String("componentReporter.registerURL", "", "Monitor center URL for vmagent component registration. "+
		"Usually it points to /monitor/components/register")
	heartbeatURL = flag.String("componentReporter.heartbeatURL", "", "Monitor center URL for vmagent component heartbeat. "+
		"Usually it points to /monitor/components/heartbeat")
	apiKey               = flagutil.NewPassword("componentReporter.apiKey", "Optional API key for monitor center component reporting")
	componentID          = flag.String("componentReporter.componentID", "vmagent", "Stable component id reported to monitor center")
	componentName        = flag.String("componentReporter.name", "vmagent", "Human-readable component name reported to monitor center")
	zone                 = flag.String("componentReporter.zone", "", "Optional zone, IDC or network domain reported to monitor center")
	endpoint             = flag.String("componentReporter.endpoint", "", "Optional externally reachable vmagent endpoint reported to monitor center. Defaults to the first HTTP listen address")
	metricsEndpoint      = flag.String("componentReporter.metricsEndpoint", "", "Optional externally reachable vmagent metrics endpoint reported to monitor center. Defaults to endpoint + /metrics from the first HTTP listen address")
	currentConfigVersion = flag.String("componentReporter.currentConfigVersion", "", "Current config version reported to monitor center")
	heartbeatInterval    = flag.Duration("componentReporter.heartbeatInterval", time.Minute, "Interval for sending vmagent component heartbeat")
)

// Snapshot contains runtime data reported to monitor center.
type Snapshot struct {
	LastError string
	Workload  map[string]interface{}
}

// SnapshotProvider returns runtime data for heartbeat payloads.
type SnapshotProvider interface {
	Snapshot() Snapshot
}

// Config contains monitor center component reporter settings.
type Config struct {
	Enabled              bool
	RegisterURL          string
	HeartbeatURL         string
	APIKey               string
	ComponentID          string
	Name                 string
	Zone                 string
	Endpoint             string
	MetricsEndpoint      string
	CurrentConfigVersion string
	HeartbeatInterval    time.Duration
	Workload             map[string]interface{}
}

// Reporter sends component registration and heartbeat payloads to monitor center.
type Reporter struct {
	cfg      Config
	client   *http.Client
	snapshot SnapshotProvider
}

type staticSnapshotProvider struct {
	snapshot Snapshot
}

func (p *staticSnapshotProvider) Snapshot() Snapshot {
	return p.snapshot
}

// NewReporter returns a new component reporter.
func NewReporter(cfg Config, snapshot SnapshotProvider) *Reporter {
	cfg = normalizeConfig(cfg)
	if snapshot == nil {
		snapshot = &staticSnapshotProvider{}
	}
	return &Reporter{
		cfg:      cfg,
		client:   &http.Client{Timeout: 10 * time.Second},
		snapshot: snapshot,
	}
}

// NewReporterFromFlags returns a reporter configured from command-line flags.
func NewReporterFromFlags(listenAddrs []string) *Reporter {
	return NewReporter(Config{
		Enabled:              *enabled,
		RegisterURL:          *registerURL,
		HeartbeatURL:         *heartbeatURL,
		APIKey:               apiKey.Get(),
		ComponentID:          *componentID,
		Name:                 *componentName,
		Zone:                 *zone,
		Endpoint:             firstNonEmptyString(*endpoint, firstHTTPListenEndpoint(listenAddrs, "")),
		MetricsEndpoint:      firstNonEmptyString(*metricsEndpoint, firstHTTPListenEndpoint(listenAddrs, "/metrics")),
		CurrentConfigVersion: *currentConfigVersion,
		HeartbeatInterval:    *heartbeatInterval,
		Workload: map[string]interface{}{
			"http_listen_addrs": listenAddrs,
		},
	}, nil)
}

// Enabled reports whether the reporter has enough configuration to send data.
func (r *Reporter) Enabled() bool {
	return r.cfg.Enabled && (r.cfg.RegisterURL != "" || r.cfg.HeartbeatURL != "")
}

// Register sends component registration payload to monitor center.
func (r *Reporter) Register() error {
	if !r.Enabled() || r.cfg.RegisterURL == "" {
		return nil
	}
	return r.post(r.cfg.RegisterURL, r.registerPayload())
}

// Heartbeat sends component heartbeat payload to monitor center.
func (r *Reporter) Heartbeat() error {
	if !r.Enabled() || r.cfg.HeartbeatURL == "" {
		return nil
	}
	return r.post(r.cfg.HeartbeatURL, r.heartbeatPayload())
}

// Run registers the component and sends heartbeats until stop is closed.
func (r *Reporter) Run(stop <-chan struct{}) {
	if !r.Enabled() {
		logger.Infof("vmagent component reporter disabled")
		return
	}
	if err := r.Register(); err != nil {
		logger.Errorf("cannot register vmagent component: %s", err)
	} else {
		logger.Infof("registered vmagent component %q", r.cfg.ComponentID)
	}
	if err := r.Heartbeat(); err != nil {
		logger.Errorf("cannot send vmagent component heartbeat: %s", err)
	}

	ticker := time.NewTicker(r.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := r.Heartbeat(); err != nil {
				logger.Errorf("cannot send vmagent component heartbeat: %s", err)
			}
		case <-stop:
			return
		}
	}
}

func (r *Reporter) post(url string, payload map[string]interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Monitor-Component-ID", r.cfg.ComponentID)
	if r.cfg.APIKey != "" {
		req.Header.Set("X-Monitor-API-Key", r.cfg.APIKey)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("monitor center returned status %d", resp.StatusCode)
	}
	return nil
}

func (r *Reporter) registerPayload() map[string]interface{} {
	return map[string]interface{}{
		"component_id":              r.cfg.ComponentID,
		"component_type":            componentType,
		"name":                      r.cfg.Name,
		"endpoint":                  r.cfg.Endpoint,
		"metrics_endpoint":          r.cfg.MetricsEndpoint,
		"zone":                      r.cfg.Zone,
		"capabilities":              []string{"http_sd", "reload", "remote_write", "metrics"},
		"version":                   buildinfo.Version,
		"heartbeat_interval_second": int(r.cfg.HeartbeatInterval.Seconds()),
		"current_config_version":    r.cfg.CurrentConfigVersion,
		"workload":                  r.workload(Snapshot{}, ""),
	}
}

func (r *Reporter) heartbeatPayload() map[string]interface{} {
	snapshot := r.snapshot.Snapshot()
	lastError := strings.TrimSpace(snapshot.LastError)
	return map[string]interface{}{
		"component_id":              r.cfg.ComponentID,
		"component_type":            componentType,
		"status":                    componentStatus(lastError),
		"version":                   buildinfo.Version,
		"endpoint":                  r.cfg.Endpoint,
		"metrics_endpoint":          r.cfg.MetricsEndpoint,
		"current_config_version":    r.cfg.CurrentConfigVersion,
		"heartbeat_interval_second": int(r.cfg.HeartbeatInterval.Seconds()),
		"last_error":                lastError,
		"workload":                  r.workload(snapshot, lastError),
	}
}

func (r *Reporter) workload(snapshot Snapshot, lastError string) map[string]interface{} {
	workload := make(map[string]interface{}, len(r.cfg.Workload)+len(snapshot.Workload)+1)
	for k, v := range r.cfg.Workload {
		workload[k] = v
	}
	for k, v := range snapshot.Workload {
		workload[k] = v
	}
	if lastError != "" {
		workload["last_error"] = lastError
	}
	return workload
}

func normalizeConfig(cfg Config) Config {
	cfg.RegisterURL = strings.TrimSpace(cfg.RegisterURL)
	cfg.HeartbeatURL = strings.TrimSpace(cfg.HeartbeatURL)
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	cfg.ComponentID = strings.TrimSpace(cfg.ComponentID)
	if cfg.ComponentID == "" {
		cfg.ComponentID = "vmagent"
	}
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		cfg.Name = cfg.ComponentID
	}
	cfg.Zone = strings.TrimSpace(cfg.Zone)
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	cfg.MetricsEndpoint = strings.TrimSpace(cfg.MetricsEndpoint)
	cfg.CurrentConfigVersion = strings.TrimSpace(cfg.CurrentConfigVersion)
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = time.Minute
	}
	return cfg
}

func componentStatus(lastError string) string {
	if strings.TrimSpace(lastError) != "" {
		return "degraded"
	}
	return "online"
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func firstHTTPListenEndpoint(listenAddrs []string, path string) string {
	for _, addr := range listenAddrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
			return strings.TrimRight(addr, "/") + path
		}
		if strings.HasPrefix(addr, ":") {
			return "http://127.0.0.1" + addr + path
		}
		return "http://" + addr + path
	}
	return ""
}
