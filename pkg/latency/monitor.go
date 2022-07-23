package latency

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

// Test the latency between the publication and the consumption of a
// message on a topic after a round trip to a remote cluster.
// A running remailer is mandatory on other cluster to send back the ping.

type Monitor struct {
	client pulsar.Client
}

type NewMonitorReq struct {
	client pulsar.Client
}

func NewMonitor(req *NewMonitorReq) Monitor {
	//TODO implement me
	panic("implement me")
}

func (m *Monitor) CheckLatency() {
	//TODO implement me
	//create msg list
	//create pub sub session
	//start list
	// start sending the messages
	//select error or complete
	// compare created msg and received calcul latency
	panic("implement me")
}
