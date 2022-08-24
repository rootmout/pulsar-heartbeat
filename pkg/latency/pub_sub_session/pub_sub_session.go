package pubsubsession

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
)

//go:generate mockery --name PubSubSession --log-level "error" --disable-version-string

type PubSubSession interface {
	GetChan() (chan []TimeStampedPayload, chan error)
	StartListening(ctx context.Context, expectedMsgCount int)
	StartSending(ctx context.Context, payloads [][]byte) []TimeStampedPayload
	ClosePubSubSession()
}

type pubSubSession struct {
	producer            pulsar.Producer
	consumer            pulsar.Consumer
	localClusterName    string
	remoteClusterName   string
	testTimestampString string

	consumedMessagesChan chan []TimeStampedPayload
	errorChan            chan error
}

type TimeStampedPayload struct {
	Payload   []byte
	TimeStamp time.Time
}

func InitPubSubSession(client pulsar.Client, topicURL *url.URL, localClusterName, remoteClusterName string) (*pubSubSession, error) {
	newSession := &pubSubSession{
		localClusterName:     localClusterName,
		remoteClusterName:    remoteClusterName,
		consumedMessagesChan: make(chan []TimeStampedPayload),
		errorChan:            make(chan error),
		testTimestampString:  strconv.Itoa(int(time.Now().Unix())),
	}

	var err error
	newSession.producer, err = client.CreateProducer(pulsar.ProducerOptions{
		Topic: topicURL.String(),
		Name:  strings.Join([]string{localClusterName, remoteClusterName, "pubsub-producer"}, "-"),
	})

	if err != nil {
		newSession.ClosePubSubSession()
		return nil, err
	}

	newSession.consumer, err = client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topicURL.String(),
		SubscriptionName:            strings.Join([]string{localClusterName, remoteClusterName, "pubsub-consumer"}, "-"),
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	})

	if err != nil {
		newSession.ClosePubSubSession()
		return nil, err
	}

	return newSession, nil
}

func (p *pubSubSession) GetChan() (chan []TimeStampedPayload, chan error) {
	return p.consumedMessagesChan, p.errorChan
}

func (p *pubSubSession) StartListening(ctx context.Context, expectedMsgCount int) {

	var receivedMsg []TimeStampedPayload

	go func() {
		for expectedMsgCount > 0 {
			msg, err := p.consumer.Receive(ctx)
			receivedTime := time.Now()

			if err != nil {
				expectedMsgCount = 0
				p.errorChan <- fmt.Errorf("consumer Receive() error: %v", err)
				break
			}

			p.consumer.Ack(msg)

			// Discard messages if it's not a reply or if he comes from another remailer.
			if msg.Properties()["issuer"] != p.localClusterName ||
				msg.Properties()["remailer"] != p.remoteClusterName ||
				msg.Properties()["type"] != "reply" ||
				msg.Properties()["testTimestamp"] != p.testTimestampString {
				continue
			}

			expectedMsgCount -= 1
			receivedMsg = append(receivedMsg, TimeStampedPayload{
				Payload:   msg.Payload(),
				TimeStamp: receivedTime,
			})
		}

		// All messages have been consumed before timeout.
		p.consumedMessagesChan <- receivedMsg
	}()
}

func (p *pubSubSession) StartSending(ctx context.Context, payloads [][]byte) []TimeStampedPayload {
	var sentMessages []TimeStampedPayload

	for _, payload := range payloads {

		asyncMsg := pulsar.ProducerMessage{
			Payload: payload,
			Properties: map[string]string{
				"issuer":        p.localClusterName,
				"remailer":      p.remoteClusterName,
				"type":          "query",
				"testTimestamp": p.testTimestampString,
			},
		}

		sentTimestamp := time.Now()
		p.producer.SendAsync(ctx, &asyncMsg, p.sendMessageCallbackFunc)

		sentMessages = append(sentMessages, TimeStampedPayload{
			Payload:   payload,
			TimeStamp: sentTimestamp,
		})
	}

	return sentMessages
}

func (p *pubSubSession) ClosePubSubSession() {
	if p.producer != nil && !reflect.ValueOf(p.producer).IsNil() {
		p.producer.Close()
	}
	if p.consumer != nil && !reflect.ValueOf(p.consumer).IsNil() {
		p.consumer.Close()
	}
}

func (p *pubSubSession) sendMessageCallbackFunc(messageId pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
	if err != nil {
		errMsg := fmt.Sprintf("fail to sent message. err: %v", err)
		p.errorChan <- errors.New(errMsg)
	}
}
