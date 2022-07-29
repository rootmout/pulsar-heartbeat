package monitor

import (
	"context"
	"errors"
	"github.com/apache/pulsar-client-go/pulsar"
	pubsubsession "github.com/datastax/pulsar-heartbeat/pkg/latency/pub_sub_session"
	"github.com/datastax/pulsar-heartbeat/pkg/payload"
	"net/url"
	"time"
)

//go:generate mockery --name Monitor --log-level "error" --disable-version-string

// Test the latency between the publication and the consumption of a
// message on a topic after a round trip to a remote cluster.
// A running remailer is mandatory on other cluster to send back the ping.

type Monitor interface {
	CheckLatency(ctx context.Context) (time.Duration, error)
}

type monitor struct {
	client            pulsar.Client
	localClusterName  string
	remoteClusterName string
	topicURL          *url.URL
	numberOfMessages  int
	prefix            string
	payloadsSizes     []string
}

type NewMonitorReq struct {
	Client            pulsar.Client
	LocalClusterName  string
	RemoteClusterName string
	TopicURL          *url.URL
	NumberOfMessages  int
	Prefix            string
	PayloadsSizes     []string
}

func NewMonitor(req *NewMonitorReq) Monitor {
	return &monitor{
		client:            req.Client,
		localClusterName:  req.LocalClusterName,
		remoteClusterName: req.RemoteClusterName,
		topicURL:          req.TopicURL,
		numberOfMessages:  req.NumberOfMessages,
		prefix:            req.Prefix,
		payloadsSizes:     req.PayloadsSizes,
	}
}

func (m *monitor) CheckLatency(ctx context.Context) (time.Duration, error) {
	pubSubSession, err := pubsubsession.InitPubSubSession(m.client, m.topicURL, m.localClusterName, m.remoteClusterName)
	if err != nil {
		return time.Duration(0), err
	}
	return m.checkLatency(pubSubSession, ctx)
}

func (m *monitor) checkLatency(session pubsubsession.PubSubSession, ctx context.Context) (time.Duration, error) {
	defer session.ClosePubSubSession()

	receivedMsgChan, errorChan := session.GetChan()

	payloadsToSend, _ := payload.AllMsgPayloads(m.prefix, m.payloadsSizes, m.numberOfMessages)

	session.StartListening(ctx, m.numberOfMessages)
	sentTimestampedPayloads := session.StartSending(ctx, payloadsToSend)

	select {
	case err := <-errorChan:
		return time.Duration(0), err
	case receivedTimestampedMsgs := <-receivedMsgChan:

		// Check if send and received messages number is the same.
		if len(receivedTimestampedMsgs) != len(sentTimestampedPayloads) {
			return time.Duration(0), errors.New("number of reply mismatch number of send request")
		}

		// Check messages order and integrity.
		for id, msg := range receivedTimestampedMsgs {
			index := m.findIndexInSlice(&sentTimestampedPayloads, msg.Payload)

			// No message with such payload has been sent: the message is corrupted.
			if index == -1 {
				return time.Duration(0), errors.New("one of the responses came back with a corrupted payload")
			}

			// Not the same index as sent time: disorder in latency check.
			if index != id {
				return time.Duration(0), errors.New("the responses arrived in a different order than the requests")
			}
		}

		// Compute the latency. If multiple messages are sent, it's just the average.
		latency := time.Duration(0)
		for i := 0; i < m.numberOfMessages; i++ {
			latency += receivedTimestampedMsgs[i].TimeStamp.Sub(sentTimestampedPayloads[i].TimeStamp)

			// No need to divide the latency to have the average if there is (for moment) only one entry.
			if i > 0 {
				latency /= 2
			}
		}

		return latency, nil
	}
}

func (m *monitor) findIndexInSlice(timestampedPayloads *[]pubsubsession.TimeStampedPayload, payload []byte) int {
	for index, timestampedPayload := range *timestampedPayloads {
		if string(timestampedPayload.Payload) == string(payload) {
			return index
		}
	}
	return -1
}
