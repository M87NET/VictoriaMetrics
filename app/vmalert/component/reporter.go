package component

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

const componentType = "vmalert"

var (
	enabled     = flag.Bool("componentReporter.enabled", false, "Whether to enable reporting vmalert component registration and heartbeat to monitor center")
	registerURL = flag.String("componentReporter.registerURL", "", "Monitor center URL for vmalert component registration. "+
		"Usually it points to /monitor/components/register")
	heartbeatURL = flag.String("componentReporter.heartbeatURL", "", "Monitor center URL for vmalert component heartbeat. "+
		"Usually it points to /monitor/components/heartbeat")
	configURL = flag.String("componentReporter.configURL", "", "Monitor center URL for pulling managed vmalert rules. "+
		"Usually it points to /monitor/components/<component_id>/config")
	apiKey                   = flagutil.NewPassword("componentReporter.apiKey", "Optional API key for monitor center component reporting")
	componentID              = flag.String("componentReporter.componentID", "vmalert", "Stable component id reported to monitor center")
	componentName            = flag.String("componentReporter.name", "vmalert", "Human-readable component name reported to monitor center")
	zone                     = flag.String("componentReporter.zone", "", "Optional zone, IDC or network domain reported to monitor center")
	managedConfigFile        = flag.String("componentReporter.managedConfigFile", "", "Local vmalert rule file atomically replaced by monitor center managed config. Defaults to the only -rule file when exactly one -rule is configured")
	currentConfigVersion     = flag.String("componentReporter.currentConfigVersion", "", "Current config version reported to monitor center")
	currentConfigVersionFile = flag.String("componentReporter.currentConfigVersionFile", "", "File containing the currently applied monitor center config version")
	heartbeatInterval        = flag.Duration("componentReporter.heartbeatInterval", time.Minute, "Interval for sending vmalert component heartbeat and checking monitor center config sync state")
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

// ApplyFunc validates, stores, and reloads a managed vmalert config version.
type ApplyFunc func(version, content string) error

// Config contains monitor center component reporter settings.
type Config struct {
	Enabled                  bool
	RegisterURL              string
	HeartbeatURL             string
	ConfigURL                string
	APIKey                   string
	ComponentID              string
	Name                     string
	Zone                     string
	Endpoint                 string
	MetricsEndpoint          string
	ManagedConfigFile        string
	CurrentConfigVersion     string
	CurrentConfigVersionFile string
	HeartbeatInterval        time.Duration
	Workload                 map[string]interface{}
	ApplyConfig              ApplyFunc
}

// ApplyOptions contains the local managed config replacement settings.
type ApplyOptions struct {
	ManagedConfigFile string
	VersionFile       string
	RuleFiles         []string
	Version           string
	Content           string
	Validate          func([]string) error
	Reload            func() error
}

// Reporter sends component registration and heartbeat payloads to monitor center.
type Reporter struct {
	cfg      Config
	client   *http.Client
	snapshot SnapshotProvider

	mu                   sync.Mutex
	currentConfigVersion string
}

type staticSnapshotProvider struct {
	snapshot Snapshot
}

type apiResponse[T any] struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
	Data    T      `json:"data"`
}

type heartbeatResponse struct {
	DesiredConfigVersion    string `json:"desired_config_version"`
	CurrentConfigVersion    string `json:"current_config_version"`
	ConfigSyncStatus        string `json:"config_sync_status"`
	NeedResync              bool   `json:"need_resync"`
	HeartbeatIntervalSecond int    `json:"heartbeat_interval_second"`
}

type configResponse struct {
	ComponentID   string `json:"component_id"`
	ComponentType string `json:"component_type"`
	Version       string `json:"version"`
	ContentHash   string `json:"content_hash"`
	Content       string `json:"content"`
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
		cfg:                  cfg,
		client:               &http.Client{Timeout: 10 * time.Second},
		snapshot:             snapshot,
		currentConfigVersion: firstNonEmptyString(readVersionFile(cfg.CurrentConfigVersionFile), cfg.CurrentConfigVersion),
	}
}

// NewReporterFromFlags returns a reporter configured from command-line flags.
func NewReporterFromFlags(listenAddrs []string, ruleFiles []string, validate func([]string) error, reload func() error) *Reporter {
	return NewReporterFromFlagsWithSnapshot(listenAddrs, ruleFiles, validate, reload, nil)
}

// NewReporterFromFlagsWithSnapshot returns a reporter configured from command-line flags and runtime snapshot provider.
func NewReporterFromFlagsWithSnapshot(listenAddrs []string, ruleFiles []string, validate func([]string) error, reload func() error, snapshot SnapshotProvider) *Reporter {
	localManagedConfigFile := strings.TrimSpace(*managedConfigFile)
	if localManagedConfigFile == "" && len(ruleFiles) == 1 {
		localManagedConfigFile = ruleFiles[0]
	}
	applyConfig := func(version, content string) error {
		return ApplyManagedConfig(ApplyOptions{
			ManagedConfigFile: localManagedConfigFile,
			VersionFile:       *currentConfigVersionFile,
			RuleFiles:         ruleFiles,
			Version:           version,
			Content:           content,
			Validate:          validate,
			Reload:            reload,
		})
	}
	return NewReporter(Config{
		Enabled:                  *enabled,
		RegisterURL:              *registerURL,
		HeartbeatURL:             *heartbeatURL,
		ConfigURL:                *configURL,
		APIKey:                   apiKey.Get(),
		ComponentID:              *componentID,
		Name:                     *componentName,
		Zone:                     *zone,
		Endpoint:                 firstHTTPListenEndpoint(listenAddrs, ""),
		MetricsEndpoint:          firstHTTPListenEndpoint(listenAddrs, "/metrics"),
		ManagedConfigFile:        localManagedConfigFile,
		CurrentConfigVersion:     *currentConfigVersion,
		CurrentConfigVersionFile: *currentConfigVersionFile,
		HeartbeatInterval:        *heartbeatInterval,
		Workload: map[string]interface{}{
			"http_listen_addrs": listenAddrs,
			"rule_files":        ruleFiles,
		},
		ApplyConfig: applyConfig,
	}, snapshot)
}

// Enabled reports whether the reporter has enough configuration to send data.
func (r *Reporter) Enabled() bool {
	return r.cfg.Enabled && (r.cfg.RegisterURL != "" || r.cfg.HeartbeatURL != "")
}

// CurrentConfigVersion returns the last applied config version reported to monitor center.
func (r *Reporter) CurrentConfigVersion() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentConfigVersion
}

// Register sends component registration payload to monitor center.
func (r *Reporter) Register() error {
	if !r.Enabled() || r.cfg.RegisterURL == "" {
		return nil
	}
	var response apiResponse[json.RawMessage]
	return r.doJSON(http.MethodPost, r.cfg.RegisterURL, nil, r.registerPayload(), &response)
}

// Heartbeat sends component heartbeat payload to monitor center.
func (r *Reporter) Heartbeat(lastError string) (heartbeatResponse, error) {
	if !r.Enabled() || r.cfg.HeartbeatURL == "" {
		return heartbeatResponse{}, nil
	}
	var response apiResponse[heartbeatResponse]
	if err := r.doJSON(http.MethodPost, r.cfg.HeartbeatURL, nil, r.heartbeatPayload(lastError), &response); err != nil {
		return heartbeatResponse{}, err
	}
	return response.Data, nil
}

// SyncOnce sends a heartbeat and applies monitor center desired config if needed.
func (r *Reporter) SyncOnce(lastError string) error {
	response, err := r.Heartbeat(lastError)
	if err != nil {
		return err
	}
	currentVersion := r.CurrentConfigVersion()
	desiredVersion := strings.TrimSpace(response.DesiredConfigVersion)
	if !response.NeedResync || desiredVersion == "" || desiredVersion == currentVersion {
		return nil
	}
	if r.cfg.ConfigURL == "" {
		return fmt.Errorf("componentReporter.configURL is required for managed config sync")
	}
	if r.cfg.ApplyConfig == nil {
		return fmt.Errorf("componentReporter managed config apply function is not configured")
	}
	remoteConfig, err := r.FetchConfig(desiredVersion)
	if err != nil {
		return err
	}
	if remoteConfig.ComponentID != "" && remoteConfig.ComponentID != r.cfg.ComponentID {
		return fmt.Errorf("monitor returned config for component %q, want %q", remoteConfig.ComponentID, r.cfg.ComponentID)
	}
	if remoteConfig.ComponentType != "" && remoteConfig.ComponentType != componentType {
		return fmt.Errorf("monitor returned config for component type %q, want %q", remoteConfig.ComponentType, componentType)
	}
	if remoteConfig.Version != desiredVersion {
		return fmt.Errorf("monitor returned config version %q, want %q", remoteConfig.Version, desiredVersion)
	}
	if err := r.cfg.ApplyConfig(remoteConfig.Version, remoteConfig.Content); err != nil {
		return err
	}
	r.setCurrentConfigVersion(remoteConfig.Version)
	return nil
}

// FetchConfig downloads a managed config version from monitor center.
func (r *Reporter) FetchConfig(version string) (configResponse, error) {
	query := url.Values{}
	if strings.TrimSpace(version) != "" {
		query.Set("version", strings.TrimSpace(version))
	}
	var response apiResponse[configResponse]
	if err := r.doJSON(http.MethodGet, r.cfg.ConfigURL, query, nil, &response); err != nil {
		return configResponse{}, err
	}
	return response.Data, nil
}

// Run registers the component and keeps heartbeats and config sync running until stop is closed.
func (r *Reporter) Run(stop <-chan struct{}) {
	if !r.Enabled() {
		logger.Infof("vmalert component reporter disabled")
		return
	}
	if err := r.Register(); err != nil {
		logger.Errorf("cannot register vmalert component: %s", err)
	} else {
		logger.Infof("registered vmalert component %q", r.cfg.ComponentID)
	}

	ticker := time.NewTicker(r.cfg.HeartbeatInterval)
	defer ticker.Stop()
	lastError := ""
	for {
		if err := r.SyncOnce(lastError); err != nil {
			lastError = err.Error()
			logger.Errorf("cannot sync vmalert component: %s", err)
		} else {
			lastError = ""
		}

		select {
		case <-ticker.C:
		case <-stop:
			return
		}
	}
}

func (r *Reporter) doJSON(method, reqURL string, query url.Values, payload interface{}, out interface{}) error {
	var body *bytes.Reader
	if payload == nil {
		body = bytes.NewReader(nil)
	} else {
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	if len(query) > 0 {
		reqURL += "?" + query.Encode()
	}
	req, err := http.NewRequest(method, reqURL, body)
	if err != nil {
		return err
	}
	req.Header.Set("X-Monitor-Component-ID", r.cfg.ComponentID)
	if r.cfg.APIKey != "" {
		req.Header.Set("X-Monitor-API-Key", r.cfg.APIKey)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
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
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (r *Reporter) registerPayload() map[string]interface{} {
	return map[string]interface{}{
		"component_id":              r.cfg.ComponentID,
		"component_type":            componentType,
		"name":                      r.cfg.Name,
		"endpoint":                  r.cfg.Endpoint,
		"metrics_endpoint":          r.cfg.MetricsEndpoint,
		"zone":                      r.cfg.Zone,
		"capabilities":              []string{"alert_rules", "config_pull", "reload", "metrics"},
		"version":                   buildinfo.Version,
		"heartbeat_interval_second": int(r.cfg.HeartbeatInterval.Seconds()),
		"current_config_version":    r.CurrentConfigVersion(),
		"workload":                  r.workload(Snapshot{}, ""),
	}
}

func (r *Reporter) heartbeatPayload(lastError string) map[string]interface{} {
	snapshot := r.snapshot.Snapshot()
	lastError = firstNonEmptyString(snapshot.LastError, lastError)
	return map[string]interface{}{
		"component_id":              r.cfg.ComponentID,
		"component_type":            componentType,
		"status":                    componentStatus(lastError),
		"version":                   buildinfo.Version,
		"endpoint":                  r.cfg.Endpoint,
		"metrics_endpoint":          r.cfg.MetricsEndpoint,
		"current_config_version":    r.CurrentConfigVersion(),
		"heartbeat_interval_second": int(r.cfg.HeartbeatInterval.Seconds()),
		"last_error":                lastError,
		"workload":                  r.workload(snapshot, lastError),
		"reported_at":               time.Now().Format(time.RFC3339Nano),
	}
}

func (r *Reporter) workload(snapshot Snapshot, lastError string) map[string]interface{} {
	workload := make(map[string]interface{}, len(r.cfg.Workload)+len(snapshot.Workload)+2)
	for k, v := range r.cfg.Workload {
		workload[k] = v
	}
	for k, v := range snapshot.Workload {
		workload[k] = v
	}
	if r.cfg.ManagedConfigFile != "" {
		workload["managed_config_file"] = r.cfg.ManagedConfigFile
	}
	if lastError != "" {
		workload["last_error"] = lastError
	}
	return workload
}

func (r *Reporter) setCurrentConfigVersion(version string) {
	r.mu.Lock()
	r.currentConfigVersion = strings.TrimSpace(version)
	r.mu.Unlock()
}

// ApplyManagedConfig validates a monitor-center managed rule config, replaces it atomically and triggers reload.
func ApplyManagedConfig(opts ApplyOptions) error {
	managedFile := strings.TrimSpace(opts.ManagedConfigFile)
	version := strings.TrimSpace(opts.Version)
	if managedFile == "" {
		return fmt.Errorf("managed config file is required")
	}
	if version == "" {
		return fmt.Errorf("config version is required")
	}
	dir := filepath.Dir(managedFile)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(managedFile)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()
	if _, err := tmp.WriteString(opts.Content); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	validationFiles := replaceManagedConfigFile(opts.RuleFiles, managedFile, tmpName)
	if opts.Validate != nil {
		if err := opts.Validate(validationFiles); err != nil {
			return err
		}
	}
	if err := os.Rename(tmpName, managedFile); err != nil {
		return err
	}
	if opts.Reload != nil {
		if err := opts.Reload(); err != nil {
			return err
		}
	}
	if strings.TrimSpace(opts.VersionFile) != "" {
		if err := os.MkdirAll(filepath.Dir(opts.VersionFile), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(opts.VersionFile, []byte(version+"\n"), 0o600); err != nil {
			return err
		}
	}
	return nil
}

func replaceManagedConfigFile(ruleFiles []string, managedFile string, replacement string) []string {
	if len(ruleFiles) == 0 {
		return []string{replacement}
	}
	res := make([]string, 0, len(ruleFiles)+1)
	replaced := false
	for _, file := range ruleFiles {
		if file == managedFile {
			res = append(res, replacement)
			replaced = true
			continue
		}
		res = append(res, file)
	}
	if !replaced {
		res = append([]string{replacement}, res...)
	}
	return res
}

func normalizeConfig(cfg Config) Config {
	cfg.RegisterURL = strings.TrimSpace(cfg.RegisterURL)
	cfg.HeartbeatURL = strings.TrimSpace(cfg.HeartbeatURL)
	cfg.ConfigURL = strings.TrimSpace(cfg.ConfigURL)
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	cfg.ComponentID = strings.TrimSpace(cfg.ComponentID)
	if cfg.ComponentID == "" {
		cfg.ComponentID = "vmalert"
	}
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		cfg.Name = cfg.ComponentID
	}
	cfg.Zone = strings.TrimSpace(cfg.Zone)
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	cfg.MetricsEndpoint = strings.TrimSpace(cfg.MetricsEndpoint)
	cfg.ManagedConfigFile = strings.TrimSpace(cfg.ManagedConfigFile)
	cfg.CurrentConfigVersion = strings.TrimSpace(cfg.CurrentConfigVersion)
	cfg.CurrentConfigVersionFile = strings.TrimSpace(cfg.CurrentConfigVersionFile)
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = time.Minute
	}
	return cfg
}

func firstHTTPListenEndpoint(addrs []string, path string) string {
	if len(addrs) == 0 {
		return ""
	}
	addr := strings.TrimSpace(addrs[0])
	if addr == "" {
		return ""
	}
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/") + path
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	return "http://" + addr + path
}

func readVersionFile(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func componentStatus(lastError string) string {
	if strings.TrimSpace(lastError) != "" {
		return "degraded"
	}
	return "online"
}
