// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	context "context"

	pulsar "github.com/apache/pulsar-client-go/pulsar"
	mock "github.com/stretchr/testify/mock"
)

// Producer is an autogenerated mock type for the Producer type
type Producer struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *Producer) Close() {
	_m.Called()
}

// Flush provides a mock function with given fields:
func (_m *Producer) Flush() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// LastSequenceID provides a mock function with given fields:
func (_m *Producer) LastSequenceID() int64 {
	ret := _m.Called()

	var r0 int64
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int64)
	}

	return r0
}

// Name provides a mock function with given fields:
func (_m *Producer) Name() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// Send provides a mock function with given fields: _a0, _a1
func (_m *Producer) Send(_a0 context.Context, _a1 *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	ret := _m.Called(_a0, _a1)

	var r0 pulsar.MessageID
	if rf, ok := ret.Get(0).(func(context.Context, *pulsar.ProducerMessage) pulsar.MessageID); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pulsar.MessageID)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *pulsar.ProducerMessage) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SendAsync provides a mock function with given fields: _a0, _a1, _a2
func (_m *Producer) SendAsync(_a0 context.Context, _a1 *pulsar.ProducerMessage, _a2 func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	_m.Called(_a0, _a1, _a2)
}

// Topic provides a mock function with given fields:
func (_m *Producer) Topic() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

type mockConstructorTestingTNewProducer interface {
	mock.TestingT
	Cleanup(func())
}

// NewProducer creates a new instance of Producer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewProducer(t mockConstructorTestingTNewProducer) *Producer {
	mock := &Producer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
