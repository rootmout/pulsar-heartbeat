package pubsubsession

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/datastax/pulsar-heartbeat/internal/mocked/pulsar/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
	"time"
)

func Test_PubSub_Success(t *testing.T) {
	localClusterName, remoteClusterName := "pulsar-01", "pulsar-02"
	payloadToSend := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	// Create a list of moked pulsar messages based on payloadToSend list.
	// This list represent the messages received that will be returned at all consumer.Receive call.
	var pulsarConsumerMsgQueue []*mocks.Message
	for _, payload := range payloadToSend {
		msg := mocks.Message{}
		msg.On("Properties").Return(map[string]string{
			"issuer":   localClusterName,
			"remailer": remoteClusterName,
			"type":     "reply",
		})
		msg.On("Payload").Return(payload)
		pulsarConsumerMsgQueue = append(pulsarConsumerMsgQueue, &msg)
	}

	// Create mocked consumer and fill it with the response messages.
	pulsarConsumer := mocks.Consumer{}
	for _, msg := range pulsarConsumerMsgQueue {
		pulsarConsumer.On("Receive", mock.AnythingOfType("*context.timerCtx")).Return(msg, nil).Once()
	}
	pulsarConsumer.On("Ack", mock.AnythingOfType("*mocks.Message")).Return()

	// Create mocked client.
	pulsarProducer := mocks.Producer{}
	pulsarProducer.On("SendAsync",
		mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("*pulsar.ProducerMessage"),
		mock.AnythingOfType("func(pulsar.MessageID, *pulsar.ProducerMessage, error)")).Return()

	// Create mocked client, he will return the previous created consumer and producer.
	pulsarClient := mocks.Client{}
	pulsarClient.On("CreateProducer", mock.AnythingOfType("pulsar.ProducerOptions")).Return(&pulsarProducer, nil)
	pulsarClient.On("Subscribe", mock.AnythingOfType("pulsar.ConsumerOptions")).Return(&pulsarConsumer, nil)

	// pubSubSession sequence
	topicURL, err := url.Parse("pulsar+ssl://tenant/namespace/topic")
	require.NoError(t, err)

	pubSubSession, err := InitPubSubSession(&pulsarClient, topicURL, localClusterName, remoteClusterName)
	require.NoError(t, err)

	receivedMsgChan, errorChan := pubSubSession.GetChan()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*time.Duration(30)))
	defer cancel()

	pubSubSession.StartListening(ctx, len(payloadToSend))
	sentPayloads := pubSubSession.StartSending(ctx, payloadToSend)
	require.Equal(t, len(payloadToSend), len(sentPayloads))

	select {
	case err := <-errorChan:
		require.NoError(t, err)
	case receivedMsgs := <-receivedMsgChan:
		// Same number of messages between input and output?
		require.Equal(t, len(payloadToSend), len(receivedMsgs))

		// Check order, and that arrival time is posterior to send time.
		for id, _ := range sentPayloads {
			require.Equal(t, sentPayloads[id].Payload, receivedMsgs[id].Payload)
			require.NotZero(t, receivedMsgs[id].TimeStamp)
			require.NotZero(t, sentPayloads[id].TimeStamp)
			require.True(t, sentPayloads[id].TimeStamp.Before(receivedMsgs[id].TimeStamp))
		}

		// Check calls made to producer.SendAsync
		pulsarProducer.AssertNumberOfCalls(t, "SendAsync", 3)
		for id, call := range pulsarProducer.Calls {
			require.IsType(t, ctx, call.Arguments.Get(0))

			refAsyncMsg := &pulsar.ProducerMessage{
				Payload: payloadToSend[id],
				Properties: map[string]string{
					"issuer":   localClusterName,
					"remailer": remoteClusterName,
					"type":     "query",
				},
			}

			// Check if context is passed as first param.
			require.IsType(t, refAsyncMsg, call.Arguments.Get(1))
			getAsyncMsg := call.Arguments.Get(1).(*pulsar.ProducerMessage)

			// It's complicated to test time as it's not a fixed value.
			// testTimestamp is verified separately then removed from properties, this way
			// the message will match the ref that has no time set.
			testTimestamp, ok := getAsyncMsg.Properties["testTimestamp"]
			require.True(t, ok)
			require.NotEqual(t, "", testTimestamp)
			delete(getAsyncMsg.Properties, "testTimestamp")

			// Compare with the ref.
			require.Equal(t, refAsyncMsg, getAsyncMsg)
		}
	}
}

func Test_InitPubSubSession_Success(t *testing.T) {
	pulsarConsumer := mocks.Consumer{}
	pulsarProducer := mocks.Producer{}

	pulsarClient := mocks.Client{}
	pulsarClient.On("CreateProducer", mock.AnythingOfType("pulsar.ProducerOptions")).Return(&pulsarProducer, nil)
	pulsarClient.On("Subscribe", mock.AnythingOfType("pulsar.ConsumerOptions")).Return(&pulsarConsumer, nil)

	topicURL, err := url.Parse("pulsar+ssl://tenant/namespace/topic")
	require.NoError(t, err)

	session, err := InitPubSubSession(&pulsarClient, topicURL, "pulsar-01", "pulsar-02")
	require.NoError(t, err)
	require.NotNil(t, session)

	require.Equal(t, &pulsarConsumer, session.consumer)
	require.Equal(t, &pulsarProducer, session.producer)
}

func Test_InitPubSubSession_FailCreatingConsumer(t *testing.T) {
	pulsarProducer := mocks.Producer{}
	pulsarProducer.On("Close")

	fakeError := errors.New("fake error")

	pulsarClient := mocks.Client{}
	pulsarClient.On("CreateProducer", mock.AnythingOfType("pulsar.ProducerOptions")).Return(&pulsarProducer, nil)
	pulsarClient.On("Subscribe", mock.AnythingOfType("pulsar.ConsumerOptions")).Return(nil, fakeError)

	topicURL, err := url.Parse("pulsar+ssl://tenant/namespace/topic")
	require.NoError(t, err)

	session, err := InitPubSubSession(&pulsarClient, topicURL, "pulsar-01", "pulsar-02")
	require.ErrorIs(t, err, fakeError)
	require.Nil(t, session)

	pulsarProducer.AssertCalled(t, "Close")
}

func Test_InitPubSubSession_FailCreatingProducer(t *testing.T) {
	fakeError := errors.New("fake error")

	pulsarClient := mocks.Client{}
	pulsarClient.On("CreateProducer", mock.AnythingOfType("pulsar.ProducerOptions")).Return(nil, fakeError)

	topicURL, err := url.Parse("pulsar+ssl://tenant/namespace/topic")
	require.NoError(t, err)

	session, err := InitPubSubSession(&pulsarClient, topicURL, "pulsar-01", "pulsar-02")
	require.ErrorIs(t, err, fakeError)
	require.Nil(t, session)
}

func Test_GetChan_Success(t *testing.T) {
	consumedMessagesChan := make(chan []TimeStampedPayload)
	errorChan := make(chan error)

	session := pubSubSession{
		consumedMessagesChan: consumedMessagesChan,
		errorChan:            errorChan,
	}

	a, b := session.GetChan()

	require.Equal(t, consumedMessagesChan, a)
	require.Equal(t, errorChan, b)
}

func Test_StartListening_Success(t *testing.T) {
	pulsarConsumer := mocks.Consumer{}
	pulsarConsumer.On("Ack", mock.AnythingOfType("*mocks.Message"))

	var pulsarMessages []pulsar.Message
	for i := 0; i < 5; i++ {
		pulsarMessage := mocks.Message{}
		pulsarMessage.On("Properties").Return(map[string]string{
			"issuer":        "pulsar-01",
			"remailer":      "pulsar-02",
			"type":          "reply",
			"testTimestamp": "<date>",
		})
		pulsarMessage.On("Payload").Return([]byte(fmt.Sprintf("payload-%d", i)))
		pulsarConsumer.On("Receive",
			mock.AnythingOfType("*context.timerCtx")).Return(&pulsarMessage, nil).Once()
		pulsarMessages = append(pulsarMessages, &pulsarMessage)
	}

	consumedMessagesChan := make(chan []TimeStampedPayload)
	errorChan := make(chan error)
	session := pubSubSession{
		consumer:             &pulsarConsumer,
		consumedMessagesChan: consumedMessagesChan,
		errorChan:            errorChan,
		localClusterName:     "pulsar-01",
		remoteClusterName:    "pulsar-02",
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()

	session.StartListening(ctx, 5)

	select {
	case err := <-errorChan:
		require.NoError(t, err)
	case receivedMsgs := <-consumedMessagesChan:
		require.Len(t, receivedMsgs, 5)
		for i := 0; i < 5; i++ {
			require.Equal(t, []byte(fmt.Sprintf("payload-%d", i)), receivedMsgs[i].Payload)
			require.NotZero(t, receivedMsgs[i].TimeStamp)
		}
	}

	pulsarConsumer.AssertNumberOfCalls(t, "Ack", 5)
	for i := 0; i < 5; i++ {
		pulsarConsumer.AssertCalled(t, "Ack", pulsarMessages[i])
	}
}

func Test_StartListening_SuccessWithNoise(t *testing.T) {
	pulsarConsumer := mocks.Consumer{}
	pulsarConsumer.On("Ack", mock.AnythingOfType("*mocks.Message"))

	var pulsarMessages []pulsar.Message

	// Noise messages (comes from another clusters or are already reply)
	noisyPulsarMessagesProperties := []map[string]string{
		{
			"issuer":        "pulsar-01",
			"remailer":      "pulsar-02",
			"type":          "query",
			"testTimestamp": "<date>",
		},
		{
			"issuer":        "pulsar-02",
			"remailer":      "pulsar-01",
			"type":          "reply",
			"testTimestamp": "<date>",
		},
		{
			"issuer":        "pulsar-02",
			"remailer":      "pulsar-01",
			"type":          "query",
			"testTimestamp": "<date>",
		},
	}
	for i := 0; i < 3; i++ {
		pulsarMessage := mocks.Message{}
		pulsarMessage.On("Properties").Return(noisyPulsarMessagesProperties[i])
		pulsarMessage.On("Payload").Return([]byte("noise-payload"))
		pulsarConsumer.On("Receive",
			mock.AnythingOfType("*context.timerCtx")).Return(&pulsarMessage, nil).Once()
		pulsarMessages = append(pulsarMessages, &pulsarMessage)
	}

	for i := 0; i < 5; i++ {
		pulsarMessage := mocks.Message{}
		pulsarMessage.On("Properties").Return(map[string]string{
			"issuer":        "pulsar-01",
			"remailer":      "pulsar-02",
			"type":          "reply",
			"testTimestamp": "<date>",
		})
		pulsarMessage.On("Payload").Return([]byte(fmt.Sprintf("payload-%d", i)))
		pulsarConsumer.On("Receive",
			mock.AnythingOfType("*context.timerCtx")).Return(&pulsarMessage, nil).Once()
		pulsarMessages = append(pulsarMessages, &pulsarMessage)
	}

	consumedMessagesChan := make(chan []TimeStampedPayload)
	errorChan := make(chan error)
	session := pubSubSession{
		consumer:             &pulsarConsumer,
		consumedMessagesChan: consumedMessagesChan,
		errorChan:            errorChan,
		localClusterName:     "pulsar-01",
		remoteClusterName:    "pulsar-02",
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()

	session.StartListening(ctx, 5)

	select {
	case err := <-errorChan:
		require.NoError(t, err)
	case receivedMsgs := <-consumedMessagesChan:
		require.Len(t, receivedMsgs, 5)
		for i := 0; i < 5; i++ {
			require.Equal(t, []byte(fmt.Sprintf("payload-%d", i)), receivedMsgs[i].Payload)
			require.NotZero(t, receivedMsgs[i].TimeStamp)
		}
	}

	// Noisy messages should be acked too.
	pulsarConsumer.AssertNumberOfCalls(t, "Ack", 5+3)
	for i := 0; i < 5+3; i++ {
		pulsarConsumer.AssertCalled(t, "Ack", pulsarMessages[i])
	}
}

func Test_StartListening_ConsumerReceiveError(t *testing.T) {
	pulsarConsumer := mocks.Consumer{}
	pulsarConsumer.On("Receive",
		mock.AnythingOfType("*context.timerCtx")).Return(nil, errors.New("fake error")).Once()

	consumedMessagesChan := make(chan []TimeStampedPayload)
	errorChan := make(chan error)
	session := pubSubSession{
		consumer:             &pulsarConsumer,
		consumedMessagesChan: consumedMessagesChan,
		errorChan:            errorChan,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()

	session.StartListening(ctx, 5)

	select {
	case err := <-errorChan:
		require.Error(t, err, "consumer Receive() error: fake erro")
	case <-consumedMessagesChan:
		t.FailNow()
	}
}

func Test_StartSending_Success(t *testing.T) {
	pulsarProducer := mocks.Producer{}
	errorChan := make(chan error)

	session := pubSubSession{
		producer:  &pulsarProducer,
		errorChan: errorChan,
	}

	pulsarProducer.On("SendAsync",
		mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("*pulsar.ProducerMessage"),
		mock.AnythingOfType("func(pulsar.MessageID, *pulsar.ProducerMessage, error)")).Run(func(args mock.Arguments) {
		go func() {
			var emptyID pulsar.MessageID
			session.sendMessageCallbackFunc(emptyID, nil, nil)
		}()
	}).Return()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()

	var payloads [][]byte
	for i := 0; i < 10; i++ {
		payloads = append(payloads, []byte(fmt.Sprintf("payload-%d", i)))
	}

	sendingStartTime := time.Now()
	timestampedPayloads := session.StartSending(ctx, payloads)
	sendingEndTime := time.Now()

	require.Len(t, timestampedPayloads, 10)
	for i := 0; i < 10; i++ {
		require.Equal(t, []byte(fmt.Sprintf("payload-%d", i)), timestampedPayloads[i].Payload)
		require.NotZero(t, timestampedPayloads[i].TimeStamp)
		require.True(t, timestampedPayloads[i].TimeStamp.Before(sendingEndTime))
		require.True(t, timestampedPayloads[i].TimeStamp.After(sendingStartTime))
	}

	select {
	case err := <-errorChan:
		require.NoError(t, err)
	default:
	}
}

func Test_StartSending_ProducerSendAsyncError(t *testing.T) {
	pulsarProducer := mocks.Producer{}
	errorChan := make(chan error)

	session := pubSubSession{
		producer:  &pulsarProducer,
		errorChan: errorChan,
	}

	pulsarProducer.On("SendAsync",
		mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("*pulsar.ProducerMessage"),
		mock.AnythingOfType("func(pulsar.MessageID, *pulsar.ProducerMessage, error)")).Run(func(args mock.Arguments) {
		go func() {
			var emptyID pulsar.MessageID
			session.sendMessageCallbackFunc(emptyID, nil, errors.New("fake error"))
		}()
	}).Return()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()

	var payloads [][]byte
	for i := 0; i < 10; i++ {
		payloads = append(payloads, []byte(fmt.Sprintf("payload-%d", i)))
	}

	sendingStartTime := time.Now()
	timestampedPayloads := session.StartSending(ctx, payloads)
	sendingEndTime := time.Now()

	require.Len(t, timestampedPayloads, 10)
	for i := 0; i < 10; i++ {
		require.Equal(t, []byte(fmt.Sprintf("payload-%d", i)), timestampedPayloads[i].Payload)
		require.NotZero(t, timestampedPayloads[i].TimeStamp)
		require.True(t, timestampedPayloads[i].TimeStamp.Before(sendingEndTime))
		require.True(t, timestampedPayloads[i].TimeStamp.After(sendingStartTime))
	}

	select {
	case err := <-errorChan:
		require.Error(t, err, "fail to sent message. err: fake error")
	default:
	}
}

func Test_ClosePubSubSession_Success(t *testing.T) {
	pulsarConsumer := mocks.Consumer{}
	pulsarConsumer.On("Close")

	pulsarProducer := mocks.Producer{}
	pulsarProducer.On("Close")

	session := pubSubSession{
		producer: &pulsarProducer,
		consumer: &pulsarConsumer,
	}

	session.ClosePubSubSession()

	pulsarConsumer.AssertCalled(t, "Close")
	pulsarProducer.AssertCalled(t, "Close")
}
