See vmagent docs [here](https://docs.victoriametrics.com/victoriametrics/vmagent/).

vmagent docs can be edited at [docs/vmagent.md](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/docs/victoriametrics/vmagent.md).

## Monitor center component reporting

This fork can report vmagent as a managed monitor-center component without changing scraping, HTTP SD, remote write or reload behavior. The reporter is disabled by default.

Example:

```sh
./vmagent \
  -remoteWrite.url=http://vmauth.example:8427/insert/0/prometheus/api/v1/write \
  -componentReporter.enabled \
  -componentReporter.componentID=vmagent-bj-01 \
  -componentReporter.name=vmagent-bj-01 \
  -componentReporter.zone=bj \
  -componentReporter.registerURL=http://monitor-center.example/monitor/components/register \
  -componentReporter.heartbeatURL=http://monitor-center.example/monitor/components/heartbeat \
  -componentReporter.currentConfigVersion=cfg-20260526-001 \
  -componentReporter.apiKey=secret
```

The registration payload uses `component_type=vmagent` and reports `http_sd`, `reload`, `remote_write` and `metrics` capabilities. Heartbeats report `online` or `degraded`, current config version, endpoint, metrics endpoint and workload metadata.
