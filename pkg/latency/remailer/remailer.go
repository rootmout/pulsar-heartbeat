package remailer

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apex/log"
	"k8s.io/apimachinery/pkg/util/rand"
	"net/url"
	"strings"
	"time"
)

//go:generate mockery --name Remailer --log-level "error" --disable-version-string

// Daemon returning ping solicitations

// In case of producing/consuming message failure, the remailer will wait a time between the above interval before
// recreating a new consumer or producer, depending on the need.
const secondsBetweenConnexionRetryMin = 10
const secondsBetweenConnexionRetryMax = 20

type Remailer interface {
	StartRemailer()
}

type remailer struct {
	client   pulsar.Client
	producer pulsar.Producer
	consumer pulsar.Consumer

	topicURL         *url.URL
	localClusterName string
}

type NewRemailerReq struct {
	Client           pulsar.Client
	TopicURL         *url.URL
	LocalClusterName string
}

func NewRemailer(req NewRemailerReq) (*remailer, error) {
	return &remailer{
		client:           req.Client,
		topicURL:         req.TopicURL,
		localClusterName: req.LocalClusterName,
	}, nil
}

// StartRemailer create a daemon that loop on remail function. All operations are moved to remail to facilitate unit testing.
func (r *remailer) StartRemailer() {
	go func() {
		for {
			time.Sleep(time.Duration(r.remail()) * time.Second)
		}
	}()
}

// remail consumes a single message and produces a reply on the same topic. As several clusters can test the latency on
// the same topic, not all messages generate a response. The value returned is a waiting time in seconds before the next
// callback to this function. This waiting time is only used when messages cannot be consumed or published
// correctly (ex: network incident), otherwise it is zero.
func (r *remailer) remail() int {
	var err error
	// Check if consumer is alive and (re)create it if not.
	if r.consumer == nil {
		r.consumer, err = r.client.Subscribe(pulsar.ConsumerOptions{
			Topic:                       r.topicURL.String(),
			SubscriptionName:            strings.Join([]string{r.localClusterName, "remailer-consumer"}, "-"),
			Type:                        pulsar.Exclusive,
			SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
		})
		if err != nil {
			delay := rand.IntnRange(secondsBetweenConnexionRetryMin, secondsBetweenConnexionRetryMax)
			log.Errorf("failed to register remailer consumer on topic %s. err: %v. retry in %ds", r.topicURL.String(), err, delay)
			r.consumer = nil
			return delay
		}
	}

	// Check if producer is alive and (re)create it if not.
	if r.producer == nil {
		r.producer, err = r.client.CreateProducer(pulsar.ProducerOptions{
			Topic: r.topicURL.String(),
			Name:  strings.Join([]string{r.localClusterName, "remailer-producer"}, "-"),
		})
		if err != nil {
			delay := rand.IntnRange(secondsBetweenConnexionRetryMin, secondsBetweenConnexionRetryMax)
			log.Errorf("failed to register remailer producer on topic %s. err: %v. retry in %ds", r.topicURL.String(), err, delay)
			r.producer = nil
			return delay
		}
	}

	message, err := r.consumer.Receive(context.Background())

	if err != nil {
		delay := rand.IntnRange(secondsBetweenConnexionRetryMin, secondsBetweenConnexionRetryMax)
		log.Errorf("remailer consumer failed to read message on topic %s. err: %v. retry in %ds", r.topicURL.String(), err, delay)
		r.consumer.Close()
		r.consumer = nil
		return delay
	}

	r.consumer.Ack(message)
	messageProperties := message.Properties()

	// In debug mode: print message properties.
	messageToString := fmt.Sprintf("remailer get message %s", message.Key())
	for key, value := range messageProperties {
		messageToString += fmt.Sprintf(" %s: %s", key, value)
	}

	// Check if message has to be processed by remailer.
	if messageProperties["type"] != "query" {
		log.Debug(fmt.Sprintf("message with id %s has been discarded by remailer as it is not a query", message.Key()))
		return 0
	}

	if messageProperties["remailer"] != r.localClusterName {
		log.Debug(fmt.Sprintf("message with id %s has been discarded by remailer as is not addressed to him", message.Key()))
		return 0
	}

	// Change type field from query to response
	messageProperties["type"] = "reply"

	response := pulsar.ProducerMessage{
		Payload:    message.Payload(),
		Properties: messageProperties,
		Key:        message.Key(),
	}

	_, err = r.producer.Send(context.Background(), &response)

	if err != nil {
		delay := rand.IntnRange(secondsBetweenConnexionRetryMin, secondsBetweenConnexionRetryMax)
		log.Errorf("remailer producer failed to sent message on topic %s. err: %v. retry in %ds", r.topicURL.String(), err, delay)
		r.producer.Close()
		r.producer = nil
		return delay
	}

	return 0
}
