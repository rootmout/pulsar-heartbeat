// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	context "context"

	pubsubsession "github.com/datastax/pulsar-heartbeat/pkg/latency/pub_sub_session"
	mock "github.com/stretchr/testify/mock"
)

// PubSubSession is an autogenerated mock type for the PubSubSession type
type PubSubSession struct {
	mock.Mock
}

// ClosePubSubSession provides a mock function with given fields:
func (_m *PubSubSession) ClosePubSubSession() {
	_m.Called()
}

// GetChan provides a mock function with given fields:
func (_m *PubSubSession) GetChan() (chan []pubsubsession.TimeStampedPayload, chan error) {
	ret := _m.Called()

	var r0 chan []pubsubsession.TimeStampedPayload
	if rf, ok := ret.Get(0).(func() chan []pubsubsession.TimeStampedPayload); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(chan []pubsubsession.TimeStampedPayload)
		}
	}

	var r1 chan error
	if rf, ok := ret.Get(1).(func() chan error); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(chan error)
		}
	}

	return r0, r1
}

// StartListening provides a mock function with given fields: ctx, expectedMsgCount
func (_m *PubSubSession) StartListening(ctx context.Context, expectedMsgCount int) {
	_m.Called(ctx, expectedMsgCount)
}

// StartSending provides a mock function with given fields: ctx, payloads
func (_m *PubSubSession) StartSending(ctx context.Context, payloads [][]byte) []pubsubsession.TimeStampedPayload {
	ret := _m.Called(ctx, payloads)

	var r0 []pubsubsession.TimeStampedPayload
	if rf, ok := ret.Get(0).(func(context.Context, [][]byte) []pubsubsession.TimeStampedPayload); ok {
		r0 = rf(ctx, payloads)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]pubsubsession.TimeStampedPayload)
		}
	}

	return r0
}

type mockConstructorTestingTNewPubSubSession interface {
	mock.TestingT
	Cleanup(func())
}

// NewPubSubSession creates a new instance of PubSubSession. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewPubSubSession(t mockConstructorTestingTNewPubSubSession) *PubSubSession {
	mock := &PubSubSession{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
