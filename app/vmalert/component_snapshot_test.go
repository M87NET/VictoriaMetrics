package main

import (
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmalert/config"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmalert/datasource"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmalert/rule"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promutil"
)

func TestVmalertSnapshotProviderReportsRuntimeSummary(t *testing.T) {
	groupCfg := config.Group{
		Name:     "componentized-vmalert",
		File:     "managed.rules.yml",
		Interval: promutil.NewDuration(time.Minute),
		Rules: []config.Rule{
			{Alert: "HighErrorRate", Expr: "up == 0"},
			{Record: "job:up:sum", Expr: "sum(up) by (job)"},
		},
	}
	group := rule.NewGroup(groupCfg, &datasource.FakeQuerier{}, time.Minute, nil)
	m := &manager{
		groups: map[uint64]*rule.Group{
			group.GetID(): group,
		},
	}

	snapshot := (&vmalertSnapshotProvider{m: m}).Snapshot()
	if got, want := snapshot.Workload["groups_count"], 1; got != want {
		t.Fatalf("groups_count=%v, want %d", got, want)
	}
	if got, want := snapshot.Workload["rules_count"], 2; got != want {
		t.Fatalf("rules_count=%v, want %d", got, want)
	}
	if got, want := snapshot.Workload["alerting_rules_count"], 1; got != want {
		t.Fatalf("alerting_rules_count=%v, want %d", got, want)
	}
	if got, want := snapshot.Workload["recording_rules_count"], 1; got != want {
		t.Fatalf("recording_rules_count=%v, want %d", got, want)
	}
	if got, want := snapshot.Workload["firing_alerts_count"], 0; got != want {
		t.Fatalf("firing_alerts_count=%v, want %d", got, want)
	}
	if got, want := snapshot.Workload["pending_alerts_count"], 0; got != want {
		t.Fatalf("pending_alerts_count=%v, want %d", got, want)
	}
}
