package latency

import (
	"context"
	"time"
)

type PulsarProducer interface {
}

type PulsarConsumer interface {
}

type PulsarMessage struct {
}

// CheckLatency sends the messages with the producer and will consume all responses with consumer.
// An error is returned if responses comme in a different order or if timeout is reached. In success case, the latency
// average will be returned.
func CheckLatency(ctx context.Context, producer *PulsarProducer, consumer *PulsarConsumer, messages []PulsarMessage) (time.Duration, error) {
	return time.Duration(0), nil
}
