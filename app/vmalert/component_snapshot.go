package main

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmalert/component"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmalert/notifier"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmalert/rule"
)

type vmalertSnapshotProvider struct {
	m *manager
}

func (p *vmalertSnapshotProvider) Snapshot() component.Snapshot {
	workload := map[string]interface{}{
		"last_reload_successful": getLastConfigError() == nil,
		"notifiers_count":        countNotifierTargets(),
	}
	if p == nil || p.m == nil {
		return component.Snapshot{Workload: workload}
	}
	p.m.groupsMu.RLock()
	defer p.m.groupsMu.RUnlock()

	var rulesCount, alertingRulesCount, recordingRulesCount, firingAlertsCount, pendingAlertsCount int
	for _, group := range p.m.groups {
		apiGroup := group.ToAPI()
		for _, apiRule := range apiGroup.Rules {
			rulesCount++
			switch apiRule.Type {
			case rule.TypeAlerting:
				alertingRulesCount++
			case rule.TypeRecording:
				recordingRulesCount++
			}
			for _, alert := range apiRule.Alerts {
				switch alert.State {
				case "firing":
					firingAlertsCount++
				case "pending":
					pendingAlertsCount++
				}
			}
		}
	}
	workload["groups_count"] = len(p.m.groups)
	workload["rules_count"] = rulesCount
	workload["alerting_rules_count"] = alertingRulesCount
	workload["recording_rules_count"] = recordingRulesCount
	workload["firing_alerts_count"] = firingAlertsCount
	workload["pending_alerts_count"] = pendingAlertsCount
	return component.Snapshot{Workload: workload}
}

func countNotifierTargets() int {
	targets := notifier.GetTargets()
	var count int
	for _, items := range targets {
		count += len(items)
	}
	return count
}
