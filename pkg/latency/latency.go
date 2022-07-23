package latency

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"time"
)

// CheckLatency sends the messages with the producer and will consume all responses with consumer.
// An error is returned if responses comme in a different order or if timeout is reached. In success case, the latency
// average will be returned.
func CheckLatency(ctx context.Context, producer pulsar.Producer, consumer pulsar.Consumer, messages []pulsar.Message) (time.Duration, error) {
	//TODO implement me
	return time.Duration(0), nil
}
