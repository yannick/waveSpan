package health

import "github.com/prometheus/client_golang/prometheus"

// PromMetrics is the Prometheus-backed Metrics sink for the disk-pressure monitor. It exposes a
// disk_pressure gauge (0=none, 1=pressure, 2=critical) and a shed-writes counter.
type PromMetrics struct {
	pressure prometheus.Gauge
	shed     prometheus.Counter
}

var _ Metrics = (*PromMetrics)(nil)

// NewPromMetrics registers the disk-pressure collectors against reg and returns the sink. The gauge
// starts at 0 (no pressure).
func NewPromMetrics(reg prometheus.Registerer) *PromMetrics {
	m := &PromMetrics{
		pressure: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wavespan_disk_pressure",
			Help: "Disk-pressure level on the storage volume: 0=none, 1=pressure (writes shed), 2=critical.",
		}),
		shed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wavespan_disk_pressure_shed_writes_total",
			Help: "Writes shed at admission because the storage volume was under disk pressure.",
		}),
	}
	reg.MustRegister(m.pressure, m.shed)
	return m
}

// SetDiskPressure records the current level as the gauge value.
func (m *PromMetrics) SetDiskPressure(level Level) { m.pressure.Set(float64(level)) }

// IncShedWrites counts one shed write.
func (m *PromMetrics) IncShedWrites() { m.shed.Inc() }
