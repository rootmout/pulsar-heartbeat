package latency

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"time"
)

type PubSubSession interface {
	//TODO
}

type pubSubSession struct {
	producer          pulsar.Producer
	consumer          pulsar.Consumer
	localClusterName  string
	remoteClusterName string
	sessionID         string
}

func InitPubSubSession(client pulsar.Client, localClusterName, remoteClusterName string) *pubSubSession {
	//TODO implement me
	// create producer/consumer
	panic("implement me")
}

func (l *pubSubSession) StartListening(ctx context.Context) (chan []struct {
	time.Duration
	pulsar.Message
}, chan error) {
	//TODO implement me
	panic("implement me")
}

func (l *pubSubSession) StartSending(ctx context.Context, messages []pulsar.Message) error {
	//TODO implement me
	// create chan and exec subroutine
	panic("implement me")
}
