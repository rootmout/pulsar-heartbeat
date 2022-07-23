package latency

import (
	"context"
	"time"
)

// Test the latency between the publication and the consumption of a
// message on a topic after a round trip to a remote cluster.
// A running remailer is mandatory on other cluster to send back the ping.

type PulsarClient interface {
}

type Monitor struct {
	checkLatencyFunction func(ctx context.Context, producer *PulsarProducer, consumer *PulsarConsumer, messages []PulsarMessage) (time.Duration, error)
}

type NewMonitorReq struct {
}

func NewMonitor(req *NewMonitorReq) Monitor {
	return Monitor{
		checkLatencyFunction: CheckLatency,
	}
}

func (m *Monitor) CheckLatency() {

}
