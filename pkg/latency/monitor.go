package latency

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"time"
)

// Test the latency between the publication and the consumption of a
// message on a topic after a round trip to a remote cluster.
// A running remailer is mandatory on other cluster to send back the ping.

type Monitor struct {
	checkLatencyFunction func(ctx context.Context, producer pulsar.Producer, consumer pulsar.Consumer, messages []pulsar.Message) (time.Duration, error)
}

type NewMonitorReq struct {
	client pulsar.Client
}

func NewMonitor(req *NewMonitorReq) Monitor {
	return Monitor{
		checkLatencyFunction: CheckLatency,
	}
}

func (m *Monitor) CheckLatency() {

}
