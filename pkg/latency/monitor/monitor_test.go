package monitor

import (
	"context"
	"errors"
	"fmt"
	pubsubsession "github.com/datastax/pulsar-heartbeat/pkg/latency/pub_sub_session"
	"github.com/datastax/pulsar-heartbeat/pkg/latency/pub_sub_session/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_checukLatency_Success(t *testing.T) {
	latencys := []int{34, 38, 43, 29, 54, 34, 45, 43, 43, 47}

	var latencyAvg time.Duration
	for i := 0; i < len(latencys); i++ {
		latencyAvg += time.Duration(latencys[i]) * time.Millisecond
		if i > 0 {
			latencyAvg /= 2
		}
	}

	clock := time.Now()
	var sentTimestampedPayload []pubsubsession.TimeStampedPayload
	for i := 0; i < 10; i++ {
		sentTimestampedPayload = append(sentTimestampedPayload, pubsubsession.TimeStampedPayload{
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			TimeStamp: clock,
		})
		clock = clock.Add(time.Duration(10) * time.Millisecond)
	}

	var receivedTimestampedPayload []pubsubsession.TimeStampedPayload
	for i := 0; i < 10; i++ {
		receivedTimestampedPayload = append(receivedTimestampedPayload, pubsubsession.TimeStampedPayload{
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			TimeStamp: sentTimestampedPayload[i].TimeStamp.Add(time.Duration(latencys[i]) * time.Millisecond),
		})
	}

	sessionReceivedMsgChan := make(chan []pubsubsession.TimeStampedPayload)
	sessionErrorChan := make(chan error)

	session := mocks.PubSubSession{}
	session.On("ClosePubSubSession")
	session.On("GetChan").Return(sessionReceivedMsgChan, sessionErrorChan)
	session.On("StartListening", mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("int")).Run(func(args mock.Arguments) {
		go func() {
			sessionReceivedMsgChan <- receivedTimestampedPayload
		}()
	})
	session.On("StartSending", mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("[][]uint8")).Run(func(args mock.Arguments) {
		payloads := args.Get(1).([][]uint8)
		for index, _ := range payloads {
			payloads[index] = sentTimestampedPayload[index].Payload
		}
	}).Return(sentTimestampedPayload)

	monitor := monitor{
		numberOfMessages: 10,
		prefix:           "prefix",
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()

	latency, err := monitor.checkLatency(&session, ctx)
	require.NoError(t, err)
	require.Equal(t, latencyAvg, latency)

	session.AssertCalled(t, "ClosePubSubSession")
}

func Test_checukLatency_FailReplyNumberMismatch(t *testing.T) {

	var sentTimestampedPayload []pubsubsession.TimeStampedPayload
	for i := 0; i < 10; i++ {
		sentTimestampedPayload = append(sentTimestampedPayload, pubsubsession.TimeStampedPayload{
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			TimeStamp: time.Now(),
		})
	}

	var receivedTimestampedPayload []pubsubsession.TimeStampedPayload
	for i := 0; i < 9; i++ {
		receivedTimestampedPayload = append(receivedTimestampedPayload, pubsubsession.TimeStampedPayload{
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			TimeStamp: time.Now(),
		})
	}

	sessionReceivedMsgChan := make(chan []pubsubsession.TimeStampedPayload)
	sessionErrorChan := make(chan error)

	session := mocks.PubSubSession{}
	session.On("ClosePubSubSession")
	session.On("GetChan").Return(sessionReceivedMsgChan, sessionErrorChan)
	session.On("StartListening", mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("int")).Run(func(args mock.Arguments) {
		go func() {
			sessionReceivedMsgChan <- receivedTimestampedPayload
		}()
	})
	session.On("StartSending", mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("[][]uint8")).Run(func(args mock.Arguments) {
		payloads := args.Get(1).([][]uint8)
		for index, _ := range payloads {
			payloads[index] = sentTimestampedPayload[index].Payload
		}
	}).Return(sentTimestampedPayload)

	monitor := monitor{
		numberOfMessages: 10,
		prefix:           "prefix",
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()

	latency, err := monitor.checkLatency(&session, ctx)
	require.EqualError(t, err, "number of reply mismatch number of send request")
	require.Zero(t, latency)

	session.AssertCalled(t, "ClosePubSubSession")
}

func Test_checukLatency_FailCorruptedMessagePayload(t *testing.T) {

	var sentTimestampedPayload []pubsubsession.TimeStampedPayload
	for i := 0; i < 10; i++ {
		sentTimestampedPayload = append(sentTimestampedPayload, pubsubsession.TimeStampedPayload{
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			TimeStamp: time.Now(),
		})
	}

	var receivedTimestampedPayload []pubsubsession.TimeStampedPayload
	for i := 0; i < 10; i++ {
		receivedTimestampedPayload = append(receivedTimestampedPayload, pubsubsession.TimeStampedPayload{
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			TimeStamp: time.Now(),
		})
	}

	// Corrupt message payload at index 3.
	receivedTimestampedPayload[3].Payload = []byte("pewjdoihefiuwefuw")

	sessionReceivedMsgChan := make(chan []pubsubsession.TimeStampedPayload)
	sessionErrorChan := make(chan error)

	session := mocks.PubSubSession{}
	session.On("ClosePubSubSession")
	session.On("GetChan").Return(sessionReceivedMsgChan, sessionErrorChan)
	session.On("StartListening", mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("int")).Run(func(args mock.Arguments) {
		go func() {
			sessionReceivedMsgChan <- receivedTimestampedPayload
		}()
	})
	session.On("StartSending", mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("[][]uint8")).Run(func(args mock.Arguments) {
		payloads := args.Get(1).([][]uint8)
		for index, _ := range payloads {
			payloads[index] = sentTimestampedPayload[index].Payload
		}
	}).Return(sentTimestampedPayload)

	monitor := monitor{
		numberOfMessages: 10,
		prefix:           "prefix",
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()

	latency, err := monitor.checkLatency(&session, ctx)
	require.EqualError(t, err, "one of the responses came back with a corrupted payload")
	require.Zero(t, latency)

	session.AssertCalled(t, "ClosePubSubSession")
}

func Test_checukLatency_FailBadOrder(t *testing.T) {

	var sentTimestampedPayload []pubsubsession.TimeStampedPayload
	for i := 0; i < 10; i++ {
		sentTimestampedPayload = append(sentTimestampedPayload, pubsubsession.TimeStampedPayload{
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			TimeStamp: time.Now(),
		})
	}

	var receivedTimestampedPayload []pubsubsession.TimeStampedPayload
	for i := 0; i < 10; i++ {
		receivedTimestampedPayload = append(receivedTimestampedPayload, pubsubsession.TimeStampedPayload{
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			TimeStamp: time.Now(),
		})
	}

	// Swap msg at index 4 and 5.
	buffer := receivedTimestampedPayload[4]
	receivedTimestampedPayload[4] = receivedTimestampedPayload[5]
	receivedTimestampedPayload[5] = buffer

	sessionReceivedMsgChan := make(chan []pubsubsession.TimeStampedPayload)
	sessionErrorChan := make(chan error)

	session := mocks.PubSubSession{}
	session.On("ClosePubSubSession")
	session.On("GetChan").Return(sessionReceivedMsgChan, sessionErrorChan)
	session.On("StartListening", mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("int")).Run(func(args mock.Arguments) {
		go func() {
			sessionReceivedMsgChan <- receivedTimestampedPayload
		}()
	})
	session.On("StartSending", mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("[][]uint8")).Run(func(args mock.Arguments) {
		payloads := args.Get(1).([][]uint8)
		for index, _ := range payloads {
			payloads[index] = sentTimestampedPayload[index].Payload
		}
	}).Return(sentTimestampedPayload)

	monitor := monitor{
		numberOfMessages: 10,
		prefix:           "prefix",
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()

	latency, err := monitor.checkLatency(&session, ctx)
	require.EqualError(t, err, "the responses arrived in a different order than the requests")
	require.Zero(t, latency)

	session.AssertCalled(t, "ClosePubSubSession")
}

func Test_checukLatency_ErrorWhilePubSub(t *testing.T) {
	sessionReceivedMsgChan := make(chan []pubsubsession.TimeStampedPayload)
	sessionErrorChan := make(chan error)

	session := mocks.PubSubSession{}
	session.On("ClosePubSubSession")
	session.On("GetChan").Return(sessionReceivedMsgChan, sessionErrorChan)
	session.On("StartListening", mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("int"))
	session.On("StartSending", mock.AnythingOfType("*context.timerCtx"),
		mock.AnythingOfType("[][]uint8")).Return(nil)

	monitor := monitor{
		numberOfMessages: 10,
		prefix:           "prefix",
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()

	go func() {
		sessionErrorChan <- errors.New("fake error")
	}()

	latency, err := monitor.checkLatency(&session, ctx)
	require.EqualError(t, err, "fake error")
	require.Zero(t, latency)

	session.AssertCalled(t, "ClosePubSubSession")
}

func Test_FindIndexInSlice_Success(t *testing.T) {
	timestampedPayloads := []pubsubsession.TimeStampedPayload{
		{
			[]byte("StringA"),
			time.Now(),
		},
		{
			[]byte("StringB"),
			time.Now(),
		},
		{
			[]byte("StringC"),
			time.Now(),
		},
	}

	monitor := monitor{}
	require.Equal(t, 2, monitor.findIndexInSlice(&timestampedPayloads, []byte("StringC")))
	require.Equal(t, 0, monitor.findIndexInSlice(&timestampedPayloads, []byte("StringA")))
	require.Equal(t, 1, monitor.findIndexInSlice(&timestampedPayloads, []byte("StringB")))
}

func Test_FindIndexInSlice_NotFound(t *testing.T) {
	timestampedPayloads := []pubsubsession.TimeStampedPayload{
		{
			[]byte("StringA"),
			time.Now(),
		},
		{
			[]byte("StringB"),
			time.Now(),
		},
		{
			[]byte("StringC"),
			time.Now(),
		},
	}

	monitor := monitor{}
	require.Equal(t, -1, monitor.findIndexInSlice(&timestampedPayloads, []byte("StringD")))
}
