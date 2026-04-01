// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package promgateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

// RegistryGatherer implements prometheus.Gatherer interface for go-ethereum's metrics.Registry
type RegistryGatherer struct {
	registry metrics.Registry
	prefix   string
}

func NewRegistryGatherer(registry metrics.Registry, prefix string) *RegistryGatherer {
	return &RegistryGatherer{
		registry: registry,
		prefix:   prefix,
	}
}

func (rg *RegistryGatherer) Gather() ([]*dto.MetricFamily, error) {
	var names []string
	rg.registry.Each(func(name string, i interface{}) {
		names = append(names, name)
	})
	sort.Strings(names)

	familyMap := make(map[string]*dto.MetricFamily)
	for _, name := range names {
		i := rg.registry.Get(name)
		if err := rg.addMetric(familyMap, name, i); err != nil {
			log.Warn("Failed to convert metric to Prometheus format", "name", name, "error", err)
		}
	}

	families := make([]*dto.MetricFamily, 0, len(familyMap))
	for _, family := range familyMap {
		families = append(families, family)
	}
	return families, nil
}

func (rg *RegistryGatherer) addMetric(familyMap map[string]*dto.MetricFamily, name string, i interface{}) error {
	name = rg.applyPrefix(sanitizeKey(name))

	switch m := i.(type) {
	case *metrics.Counter:
		rg.addGaugeMetric(familyMap, name, float64(m.Snapshot().Count()))
	case *metrics.CounterFloat64:
		rg.addGaugeMetric(familyMap, name, m.Snapshot().Count())
	case *metrics.Gauge:
		rg.addGaugeMetric(familyMap, name, float64(m.Snapshot().Value()))
	case *metrics.GaugeFloat64:
		rg.addGaugeMetric(familyMap, name, m.Snapshot().Value())
	case *metrics.GaugeInfo:
		rg.addGaugeInfoMetric(familyMap, name, m.Snapshot().Value())
	case metrics.Histogram:
		rg.addHistogramMetric(familyMap, name, m.Snapshot())
	case *metrics.Meter:
		rg.addGaugeMetric(familyMap, name, float64(m.Snapshot().Count()))
	case *metrics.Timer:
		rg.addTimerMetric(familyMap, name, m.Snapshot())
	case *metrics.ResettingTimer:
		if m.Snapshot().Count() > 0 {
			rg.addResettingTimerMetric(familyMap, name, m.Snapshot())
		}
	default:
		return fmt.Errorf("unknown metric type %T", i)
	}
	return nil
}

func (rg *RegistryGatherer) addGaugeMetric(familyMap map[string]*dto.MetricFamily, name string, value float64) {
	metricType := dto.MetricType_GAUGE
	family := rg.getOrCreateFamily(familyMap, name, metricType)
	family.Metric = append(family.Metric, &dto.Metric{
		Gauge: &dto.Gauge{Value: &value},
	})
}

func (rg *RegistryGatherer) addGaugeInfoMetric(familyMap map[string]*dto.MetricFamily, name string, value metrics.GaugeInfoValue) {
	metricType := dto.MetricType_GAUGE
	family := rg.getOrCreateFamily(familyMap, name, metricType)

	labels := make([]*dto.LabelPair, 0, len(value))
	for k, v := range value {
		labelName := k
		labelValue := v
		labels = append(labels, &dto.LabelPair{Name: &labelName, Value: &labelValue})
	}
	sort.Slice(labels, func(i, j int) bool { return *labels[i].Name < *labels[j].Name })

	gaugeValue := float64(1)
	family.Metric = append(family.Metric, &dto.Metric{
		Label: labels,
		Gauge: &dto.Gauge{Value: &gaugeValue},
	})
}

func (rg *RegistryGatherer) addSummaryMetric(familyMap map[string]*dto.MetricFamily, name string, count int64, percentileValues []float64, percentileKeys []float64) {
	metricType := dto.MetricType_SUMMARY
	family := rg.getOrCreateFamily(familyMap, name, metricType)

	quantiles := make([]*dto.Quantile, len(percentileKeys))
	for i := range percentileKeys {
		q := percentileKeys[i]
		v := percentileValues[i]
		quantiles[i] = &dto.Quantile{Quantile: &q, Value: &v}
	}

	ucount := uint64(count) //nolint:gosec
	family.Metric = append(family.Metric, &dto.Metric{
		Summary: &dto.Summary{SampleCount: &ucount, Quantile: quantiles},
	})
}

var defaultPercentiles = []float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999}

func (rg *RegistryGatherer) addHistogramMetric(familyMap map[string]*dto.MetricFamily, name string, h metrics.HistogramSnapshot) {
	rg.addSummaryMetric(familyMap, name, h.Count(), h.Percentiles(defaultPercentiles), defaultPercentiles)
}

func (rg *RegistryGatherer) addTimerMetric(familyMap map[string]*dto.MetricFamily, name string, t *metrics.TimerSnapshot) {
	rg.addSummaryMetric(familyMap, name, t.Count(), t.Percentiles(defaultPercentiles), defaultPercentiles)
}

func (rg *RegistryGatherer) addResettingTimerMetric(familyMap map[string]*dto.MetricFamily, name string, t *metrics.ResettingTimerSnapshot) {
	rg.addSummaryMetric(familyMap, name, int64(t.Count()), t.Percentiles(defaultPercentiles), defaultPercentiles)
}

func (rg *RegistryGatherer) getOrCreateFamily(familyMap map[string]*dto.MetricFamily, name string, metricType dto.MetricType) *dto.MetricFamily {
	family, exists := familyMap[name]
	if !exists {
		family = &dto.MetricFamily{Name: &name, Type: &metricType}
		familyMap[name] = family
	}
	return family
}

func sanitizeKey(key string) string {
	return strings.ReplaceAll(key, "/", "_")
}

func (rg *RegistryGatherer) applyPrefix(name string) string {
	if rg.prefix != "" {
		return rg.prefix + "_" + name
	}
	return name
}

// pushgatewayPusher manages pushes to the Prometheus Pushgateway.
// Unlike the standard prometheus push.Pusher, this does NOT add a /metrics
// prefix to the URL — the base URL should already include it if needed.
type pushgatewayPusher struct {
	error error

	url      string
	job      string
	grouping map[string]string

	gatherers prometheus.Gatherers
	client    httpDoer
	expfmt    expfmt.Format
}

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

const base64Suffix = "@base64"

var errJobEmpty = errors.New("job name is empty")

func newPushgatewayPusher(pushURL string, job string) *pushgatewayPusher {
	var err error
	if job == "" {
		err = errJobEmpty
	}
	if !strings.Contains(pushURL, "://") {
		pushURL = "http://" + pushURL
	}
	pushURL = strings.TrimSuffix(pushURL, "/")

	return &pushgatewayPusher{
		error:     err,
		url:       pushURL,
		job:       job,
		grouping:  map[string]string{},
		gatherers: prometheus.Gatherers{},
		client:    &http.Client{},
		expfmt:    expfmt.FmtProtoDelim,
	}
}

func (p *pushgatewayPusher) gatherer(g prometheus.Gatherer) *pushgatewayPusher {
	p.gatherers = append(p.gatherers, g)
	return p
}

func (p *pushgatewayPusher) groupingLabel(name string, value string) *pushgatewayPusher {
	if p.error == nil {
		if !model.LabelName(name).IsValid() {
			p.error = fmt.Errorf("grouping label has invalid name: %s", name)
			return p
		}
		p.grouping[name] = value
	}
	return p
}

func (p *pushgatewayPusher) push(ctx context.Context) error {
	if p.error != nil {
		return p.error
	}
	mfs, err := p.gatherers.Gather()
	if err != nil {
		return err
	}
	buf := &bytes.Buffer{}
	enc := expfmt.NewEncoder(buf, p.expfmt)
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "job" {
					return fmt.Errorf("pushed metric %s (%s) already contains a job label", mf.GetName(), m)
				}
				if _, ok := p.grouping[l.GetName()]; ok {
					return fmt.Errorf("pushed metric %s (%s) already contains grouping label %s", mf.GetName(), m, l.GetName())
				}
			}
		}
		if err := enc.Encode(mf); err != nil {
			return fmt.Errorf("failed to encode metric family %s: %w", mf.GetName(), err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.fullURL(), buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", string(p.expfmt))
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code %d while pushing to %s: %s", resp.StatusCode, p.fullURL(), body)
	}
	return nil
}

func (p *pushgatewayPusher) fullURL() string {
	urlComponents := []string{}
	if encoded, isBase64 := encodeComponent(p.job); isBase64 {
		urlComponents = append(urlComponents, "job"+base64Suffix, encoded)
	} else {
		urlComponents = append(urlComponents, "job", encoded)
	}
	for ln, lv := range p.grouping {
		if encoded, isBase64 := encodeComponent(lv); isBase64 {
			urlComponents = append(urlComponents, ln+base64Suffix, encoded)
		} else {
			urlComponents = append(urlComponents, ln, encoded)
		}
	}
	return fmt.Sprintf("%s/%s", p.url, strings.Join(urlComponents, "/"))
}

func encodeComponent(s string) (string, bool) {
	if s == "" {
		return "=", true
	}
	if strings.Contains(s, "/") {
		return base64.RawURLEncoding.EncodeToString([]byte(s)), true
	}
	return url.QueryEscape(s), false
}

// StartPusher starts a background goroutine that periodically pushes
// go-ethereum metrics to a Prometheus Pushgateway. Returns a cleanup function.
func StartPusher(ctx context.Context, cfg *Config, registry metrics.Registry) func() {
	gatherer := NewRegistryGatherer(registry, cfg.Prefix)
	pusher := newPushgatewayPusher(cfg.URL, cfg.JobName).gatherer(gatherer)
	if cfg.Instance != "" {
		pusher = pusher.groupingLabel("instance", cfg.Instance)
	}

	pusherCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		ticker := time.NewTicker(cfg.UpdateInterval)
		defer ticker.Stop()

		if err := pusher.push(pusherCtx); err != nil {
			log.Error("Failed to push metrics to Prometheus Pushgateway", "url", cfg.URL, "error", err)
		} else {
			log.Info("Successfully pushed metrics to Prometheus Pushgateway", "url", cfg.URL, "job", cfg.JobName)
		}

		for {
			select {
			case <-pusherCtx.Done():
				log.Info("Stopping Prometheus Pushgateway pusher")
				return
			case <-ticker.C:
				if err := pusher.push(pusherCtx); err != nil {
					log.Error("Failed to push metrics to Prometheus Pushgateway", "url", cfg.URL, "error", err)
				}
			}
		}
	}()

	return func() {
		cancel()
		wg.Wait()
	}
}
