package remailer

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/datastax/pulsar-heartbeat/internal/mocked/pulsar/mocks"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"net/url"
	"strings"
	"testing"
)

func Test_NewRemailer_Type(t *testing.T) {
	pulsarClient := mocks.Client{}
	topicURL, err := url.Parse("persistent://my-tenant/my-namespace/my-topic")
	require.NoError(t, err)

	testRemailer, err := NewRemailer(NewRemailerReq{
		Client:           &pulsarClient,
		TopicURL:         topicURL,
		LocalClusterName: "pulsar-01",
	})

	require.NoError(t, err)
	require.NotNil(t, testRemailer)
	require.IsType(t, &remailer{}, testRemailer)
}

func Test_Remail_Success(t *testing.T) {
	// Step 1: Setup mocked objects.
	pulsarProducer := mocks.Producer{}
	pulsarProducer.On("Send", mock.AnythingOfType("*context.emptyCtx"), mock.AnythingOfType("*pulsar.ProducerMessage")).Return(nil, nil)
	pulsarProducer.On("Close").Return()

	pulsarMessagePropertiesTemplate := map[string]string{
		"issuer":        "pulsar-02",
		"remailer":      "pulsar-01",
		"type":          "query",
		"testStartTime": "<date>",
	}
	pulsarMessage := mocks.Message{}
	var pulsarMessageProperties map[string]string
	err := copier.Copy(&pulsarMessageProperties, &pulsarMessagePropertiesTemplate)
	require.NoError(t, err)
	pulsarMessage.On("Properties").Return(pulsarMessageProperties)
	pulsarMessage.On("Payload").Return([]byte("message-payload"))
	pulsarMessage.On("Key").Return("message-key")

	pulsarConsumer := mocks.Consumer{}
	pulsarConsumer.On("Receive", mock.AnythingOfType("*context.emptyCtx")).Return(&pulsarMessage, nil)
	pulsarConsumer.On("Close").Return()
	pulsarConsumer.On("Ack", mock.AnythingOfType("*mocks.Message")).Return()

	pulsarClient := mocks.Client{}
	pulsarClient.On("Subscribe", mock.AnythingOfType("pulsar.ConsumerOptions")).Return(&pulsarConsumer, nil)
	pulsarClient.On("CreateProducer", mock.AnythingOfType("pulsar.ProducerOptions")).Return(&pulsarProducer, nil)

	topicURL, err := url.Parse("persistent://my-tenant/my-namespace/my-topic")
	require.NoError(t, err)

	// Step 2: Create tested object.
	testRemailer, err := NewRemailer(NewRemailerReq{
		Client:           &pulsarClient,
		TopicURL:         topicURL,
		LocalClusterName: "pulsar-01",
	})
	require.NoError(t, err)
	require.NotNil(t, testRemailer)

	// Step 3: Run functions.
	sleepDelay := testRemailer.remail()

	// Step 4: Assert the results.
	// O mean success
	require.Equal(t, 0, sleepDelay)

	// client/consumer/producer must still open.
	require.NotNil(t, testRemailer.consumer)
	require.NotNil(t, testRemailer.producer)
	require.NotNil(t, testRemailer.client)

	consumerOptionsRef := pulsar.ConsumerOptions{
		Topic:                       topicURL.String(),
		SubscriptionName:            strings.Join([]string{"pulsar-01", "remailer-consumer"}, "-"),
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	}
	pulsarClient.AssertCalled(t, "Subscribe", consumerOptionsRef)

	producerOptionsRef := pulsar.ProducerOptions{
		Topic: topicURL.String(),
		Name:  "pulsar-01-remailer-producer",
	}
	pulsarClient.AssertCalled(t, "CreateProducer", producerOptionsRef)

	pulsarMessagePropertiesTemplate["type"] = "reply"
	replyMessageRef := pulsar.ProducerMessage{
		Properties: pulsarMessagePropertiesTemplate,
		Payload:    []byte("message-payload"),
		Key:        "message-key",
	}
	pulsarProducer.AssertNumberOfCalls(t, "Send", 1)
	require.IsType(t, context.Background(), pulsarProducer.Calls[0].Arguments.Get(0))
	require.Equal(t, &replyMessageRef, pulsarProducer.Calls[0].Arguments.Get(1))

	pulsarConsumer.AssertCalled(t, "Ack", &pulsarMessage)
	pulsarConsumer.AssertCalled(t, "Receive", context.Background())
}

func Test_Remail_SuccessMultipleMessages(t *testing.T) {
	// Step 1: Setup mocked objects.
	pulsarProducer := mocks.Producer{}
	pulsarProducer.On("Send", mock.AnythingOfType("*context.emptyCtx"), mock.AnythingOfType("*pulsar.ProducerMessage")).Return(nil, nil)
	pulsarProducer.On("Close").Return()

	pulsarConsumer := mocks.Consumer{}
	pulsarConsumer.On("Close").Return()
	pulsarConsumer.On("Ack", mock.AnythingOfType("*mocks.Message")).Return()

	pulsarMessagePropertiesTemplate := map[string]string{
		"issuer":        "pulsar-02",
		"remailer":      "pulsar-01",
		"type":          "query",
		"testStartTime": "<date>",
	}

	var pulsarMessages []*mocks.Message
	for i := 0; i < 10; i++ {
		var pulsarMessageProperties map[string]string
		err := copier.Copy(&pulsarMessageProperties, &pulsarMessagePropertiesTemplate)
		require.NoError(t, err)
		pulsarMessage := mocks.Message{}
		pulsarMessage.On("Properties").Return(pulsarMessageProperties)
		pulsarMessage.On("Payload").Return([]byte(fmt.Sprintf("message-payload-%d", i)))
		pulsarMessage.On("Key").Return(fmt.Sprintf("message-key-%d", i))
		pulsarConsumer.On("Receive", mock.AnythingOfType("*context.emptyCtx")).Return(&pulsarMessage, nil).Once()
		pulsarMessages = append(pulsarMessages, &pulsarMessage)
	}

	pulsarClient := mocks.Client{}
	pulsarClient.On("Subscribe", mock.AnythingOfType("pulsar.ConsumerOptions")).Return(&pulsarConsumer, nil)
	pulsarClient.On("CreateProducer", mock.AnythingOfType("pulsar.ProducerOptions")).Return(&pulsarProducer, nil)

	topicURL, err := url.Parse("persistent://my-tenant/my-namespace/my-topic")
	require.NoError(t, err)

	// Step 2: Create tested object.
	testRemailer, err := NewRemailer(NewRemailerReq{
		Client:           &pulsarClient,
		TopicURL:         topicURL,
		LocalClusterName: "pulsar-01",
	})
	require.NoError(t, err)
	require.NotNil(t, testRemailer)

	// Step 3: Run functions.
	var sleepDelay int
	for i := 0; i < 10; i++ {
		sleepDelay += testRemailer.remail()
	}

	// Step 4: Assert the results.
	// O mean success
	require.Equal(t, 0, sleepDelay)

	// client/consumer/producer must still open.
	require.NotNil(t, testRemailer.consumer)
	require.NotNil(t, testRemailer.producer)
	require.NotNil(t, testRemailer.client)

	consumerOptionsRef := pulsar.ConsumerOptions{
		Topic:                       topicURL.String(),
		SubscriptionName:            strings.Join([]string{"pulsar-01", "remailer-consumer"}, "-"),
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	}
	pulsarClient.AssertCalled(t, "Subscribe", consumerOptionsRef)

	producerOptionsRef := pulsar.ProducerOptions{
		Topic: topicURL.String(),
		Name:  "pulsar-01-remailer-producer",
	}
	pulsarClient.AssertCalled(t, "CreateProducer", producerOptionsRef)
	pulsarConsumer.AssertCalled(t, "Receive", context.Background())

	pulsarProducer.AssertNumberOfCalls(t, "Send", 10)
	pulsarConsumer.AssertNumberOfCalls(t, "Receive", 10)

	pulsarMessagePropertiesTemplate["type"] = "reply"
	for i := 0; i < 10; i++ {
		replyMessageRef := pulsar.ProducerMessage{
			Properties: pulsarMessagePropertiesTemplate,
			Payload:    []byte(fmt.Sprintf("message-payload-%d", i)),
			Key:        fmt.Sprintf("message-key-%d", i),
		}

		require.IsType(t, context.Background(), pulsarProducer.Calls[i].Arguments.Get(0))
		require.Equal(t, &replyMessageRef, pulsarProducer.Calls[i].Arguments.Get(1))

		pulsarConsumer.AssertCalled(t, "Ack", pulsarMessages[i])
	}
}

func Test_Remail_SuccessMultipleMessagesWithNoise(t *testing.T) {
	// Step 1: Setup mocked objects.
	pulsarProducer := mocks.Producer{}
	pulsarProducer.On("Send", mock.AnythingOfType("*context.emptyCtx"), mock.AnythingOfType("*pulsar.ProducerMessage")).Return(nil, nil)
	pulsarProducer.On("Close").Return()

	pulsarConsumer := mocks.Consumer{}
	pulsarConsumer.On("Close").Return()
	pulsarConsumer.On("Ack", mock.AnythingOfType("*mocks.Message")).Return()

	pulsarMessagePropertiesTemplate := map[string]string{
		"issuer":        "pulsar-02",
		"remailer":      "pulsar-01",
		"type":          "query",
		"testStartTime": "<date>",
	}

	var pulsarMessages []*mocks.Message
	for i := 0; i < 10; i++ {
		var pulsarMessageProperties map[string]string
		err := copier.Copy(&pulsarMessageProperties, &pulsarMessagePropertiesTemplate)
		require.NoError(t, err)
		pulsarMessage := mocks.Message{}
		pulsarMessage.On("Properties").Return(pulsarMessageProperties)
		pulsarMessage.On("Payload").Return([]byte(fmt.Sprintf("message-payload-%d", i)))
		pulsarMessage.On("Key").Return(fmt.Sprintf("message-key-%d", i))
		pulsarConsumer.On("Receive", mock.AnythingOfType("*context.emptyCtx")).Return(&pulsarMessage, nil).Once()
		pulsarMessages = append(pulsarMessages, &pulsarMessage)
	}

	NoiseMessageParam := []map[string]string{
		{
			"issuer":        "pulsar-02",
			"remailer":      "pulsar-03",
			"type":          "query",
			"testStartTime": "<date>",
		},
		{
			"issuer":        "pulsar-02",
			"remailer":      "pulsar-01",
			"type":          "reply",
			"testStartTime": "<date>",
		},
		{
			"issuer":        "pulsar-02",
			"remailer":      "pulsar-03",
			"type":          "reply",
			"testStartTime": "<date>",
		},
	}

	for i := 0; i < 3; i++ {
		pulsarMessage := mocks.Message{}
		pulsarMessage.On("Properties").Return(NoiseMessageParam[i])
		pulsarConsumer.On("Receive", mock.AnythingOfType("*context.emptyCtx")).Return(&pulsarMessage, nil).Once()
		pulsarMessage.On("Key").Return("noise")
		pulsarMessages = append(pulsarMessages, &pulsarMessage)
	}

	pulsarClient := mocks.Client{}
	pulsarClient.On("Subscribe", mock.AnythingOfType("pulsar.ConsumerOptions")).Return(&pulsarConsumer, nil)
	pulsarClient.On("CreateProducer", mock.AnythingOfType("pulsar.ProducerOptions")).Return(&pulsarProducer, nil)

	topicURL, err := url.Parse("persistent://my-tenant/my-namespace/my-topic")
	require.NoError(t, err)

	// Step 2: Create tested object.
	testRemailer, err := NewRemailer(NewRemailerReq{
		Client:           &pulsarClient,
		TopicURL:         topicURL,
		LocalClusterName: "pulsar-01",
	})
	require.NoError(t, err)
	require.NotNil(t, testRemailer)

	// Step 3: Run functions.
	var sleepDelay int
	for i := 0; i < 13; i++ {
		sleepDelay += testRemailer.remail()
	}

	// Step 4: Assert the results.
	// O mean success
	require.Equal(t, 0, sleepDelay)

	// client/consumer/producer must still open.
	require.NotNil(t, testRemailer.consumer)
	require.NotNil(t, testRemailer.producer)
	require.NotNil(t, testRemailer.client)

	consumerOptionsRef := pulsar.ConsumerOptions{
		Topic:                       topicURL.String(),
		SubscriptionName:            strings.Join([]string{"pulsar-01", "remailer-consumer"}, "-"),
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	}
	pulsarClient.AssertCalled(t, "Subscribe", consumerOptionsRef)

	producerOptionsRef := pulsar.ProducerOptions{
		Topic: topicURL.String(),
		Name:  "pulsar-01-remailer-producer",
	}
	pulsarClient.AssertCalled(t, "CreateProducer", producerOptionsRef)
	pulsarConsumer.AssertCalled(t, "Receive", context.Background())

	pulsarProducer.AssertNumberOfCalls(t, "Send", 10)
	pulsarConsumer.AssertNumberOfCalls(t, "Receive", 13)

	pulsarMessagePropertiesTemplate["type"] = "reply"
	for i := 0; i < 10; i++ {
		replyMessageRef := pulsar.ProducerMessage{
			Properties: pulsarMessagePropertiesTemplate,
			Payload:    []byte(fmt.Sprintf("message-payload-%d", i)),
			Key:        fmt.Sprintf("message-key-%d", i),
		}

		require.IsType(t, context.Background(), pulsarProducer.Calls[i].Arguments.Get(0))
		require.Equal(t, &replyMessageRef, pulsarProducer.Calls[i].Arguments.Get(1))

		pulsarConsumer.AssertCalled(t, "Ack", pulsarMessages[i])
	}

	for i := 10; i < 13; i++ {
		// Even if message is not send back, they should be ack.
		pulsarConsumer.AssertCalled(t, "Ack", pulsarMessages[i])
	}
}

func Test_Remail_FailToCreateConsumer(t *testing.T) {
	pulsarClient := mocks.Client{}
	pulsarClient.On("Subscribe", mock.AnythingOfType("pulsar.ConsumerOptions")).Return(nil, errors.New("error"))
	topicURL, err := url.Parse("persistent://my-tenant/my-namespace/my-topic")
	require.NoError(t, err)

	testRemailer, err := NewRemailer(NewRemailerReq{
		Client:           &pulsarClient,
		TopicURL:         topicURL,
		LocalClusterName: "pulsar-01",
	})

	require.NoError(t, err)
	require.NotNil(t, testRemailer)

	sleepDelay := testRemailer.remail()

	require.Nil(t, testRemailer.consumer)
	require.Nil(t, testRemailer.producer)
	require.LessOrEqual(t, sleepDelay, secondsBetweenConnexionRetryMax)
	require.GreaterOrEqual(t, sleepDelay, secondsBetweenConnexionRetryMin)
}

func Test_Remail_FailToCreateProducer(t *testing.T) {
	pulsarConsumer := mocks.Consumer{}

	pulsarClient := mocks.Client{}
	pulsarClient.On("Subscribe", mock.AnythingOfType("pulsar.ConsumerOptions")).Return(&pulsarConsumer, nil)
	pulsarClient.On("CreateProducer", mock.AnythingOfType("pulsar.ProducerOptions")).Return(nil, errors.New("error"))
	topicURL, err := url.Parse("persistent://my-tenant/my-namespace/my-topic")
	require.NoError(t, err)

	testRemailer, err := NewRemailer(NewRemailerReq{
		Client:           &pulsarClient,
		TopicURL:         topicURL,
		LocalClusterName: "pulsar-01",
	})

	require.NoError(t, err)
	require.NotNil(t, testRemailer)

	sleepDelay := testRemailer.remail()

	require.Equal(t, &pulsarConsumer, testRemailer.consumer)
	require.Nil(t, testRemailer.producer)
	require.LessOrEqual(t, sleepDelay, secondsBetweenConnexionRetryMax)
	require.GreaterOrEqual(t, sleepDelay, secondsBetweenConnexionRetryMin)
}

func Test_Remail_FailToReadMessage(t *testing.T) {
	pulsarConsumer := mocks.Consumer{}
	pulsarConsumer.On("Receive", mock.AnythingOfType("*context.emptyCtx")).Return(nil, errors.New("error"))
	pulsarConsumer.On("Close").Return()

	pulsarProducer := mocks.Producer{}

	pulsarClient := mocks.Client{}
	pulsarClient.On("Subscribe", mock.AnythingOfType("pulsar.ConsumerOptions")).Return(&pulsarConsumer, nil)
	pulsarClient.On("CreateProducer", mock.AnythingOfType("pulsar.ProducerOptions")).Return(&pulsarProducer, nil)
	topicURL, err := url.Parse("persistent://my-tenant/my-namespace/my-topic")
	require.NoError(t, err)

	testRemailer, err := NewRemailer(NewRemailerReq{
		Client:           &pulsarClient,
		TopicURL:         topicURL,
		LocalClusterName: "pulsar-01",
	})

	require.NoError(t, err)
	require.NotNil(t, testRemailer)

	sleepDelay := testRemailer.remail()

	require.LessOrEqual(t, sleepDelay, secondsBetweenConnexionRetryMax)
	require.GreaterOrEqual(t, sleepDelay, secondsBetweenConnexionRetryMin)

	require.Nil(t, testRemailer.consumer)
	require.Equal(t, &pulsarProducer, testRemailer.producer)

	pulsarConsumer.AssertCalled(t, "Close")
}

func Test_Remail_FailToSendMessage(t *testing.T) {
	pulsarMessageProperties := map[string]string{
		"issuer":        "pulsar-02",
		"remailer":      "pulsar-01",
		"type":          "query",
		"testStartTime": "<date>",
	}
	pulsarMessage := mocks.Message{}
	pulsarMessage.On("Properties").Return(pulsarMessageProperties)
	pulsarMessage.On("Payload").Return([]byte("message-payload"))
	pulsarMessage.On("Key").Return("message-key")

	pulsarConsumer := mocks.Consumer{}
	pulsarConsumer.On("Receive", mock.AnythingOfType("*context.emptyCtx")).Return(&pulsarMessage, nil)
	pulsarConsumer.On("Ack", mock.AnythingOfType("*mocks.Message")).Return()

	pulsarProducer := mocks.Producer{}
	pulsarProducer.On("Send", mock.AnythingOfType("*context.emptyCtx"), mock.AnythingOfType("*pulsar.ProducerMessage")).Return(nil, errors.New("error"))
	pulsarProducer.On("Close").Return()

	pulsarClient := mocks.Client{}
	pulsarClient.On("Subscribe", mock.AnythingOfType("pulsar.ConsumerOptions")).Return(&pulsarConsumer, nil)
	pulsarClient.On("CreateProducer", mock.AnythingOfType("pulsar.ProducerOptions")).Return(&pulsarProducer, nil)
	topicURL, err := url.Parse("persistent://my-tenant/my-namespace/my-topic")
	require.NoError(t, err)

	testRemailer, err := NewRemailer(NewRemailerReq{
		Client:           &pulsarClient,
		TopicURL:         topicURL,
		LocalClusterName: "pulsar-01",
	})

	require.NoError(t, err)
	require.NotNil(t, testRemailer)

	sleepDelay := testRemailer.remail()

	require.LessOrEqual(t, sleepDelay, secondsBetweenConnexionRetryMax)
	require.GreaterOrEqual(t, sleepDelay, secondsBetweenConnexionRetryMin)

	require.Equal(t, &pulsarConsumer, testRemailer.consumer)
	require.Nil(t, testRemailer.producer)

	pulsarProducer.AssertCalled(t, "Close")
}
