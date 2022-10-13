// Code generated by mockery. DO NOT EDIT.

package mocks

import (
	pulsar "github.com/apache/pulsar-client-go/pulsar"
	mock "github.com/stretchr/testify/mock"

	time "time"
)

// Message is an autogenerated mock type for the Message type
type Message struct {
	mock.Mock
}

// BrokerPublishTime provides a mock function with given fields:
func (_m *Message) BrokerPublishTime() *time.Time {
	ret := _m.Called()

	var r0 *time.Time
	if rf, ok := ret.Get(0).(func() *time.Time); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*time.Time)
		}
	}

	return r0
}

// EventTime provides a mock function with given fields:
func (_m *Message) EventTime() time.Time {
	ret := _m.Called()

	var r0 time.Time
	if rf, ok := ret.Get(0).(func() time.Time); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Time)
	}

	return r0
}

// GetEncryptionContext provides a mock function with given fields:
func (_m *Message) GetEncryptionContext() *pulsar.EncryptionContext {
	ret := _m.Called()

	var r0 *pulsar.EncryptionContext
	if rf, ok := ret.Get(0).(func() *pulsar.EncryptionContext); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*pulsar.EncryptionContext)
		}
	}

	return r0
}

// GetReplicatedFrom provides a mock function with given fields:
func (_m *Message) GetReplicatedFrom() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// GetSchemaValue provides a mock function with given fields: v
func (_m *Message) GetSchemaValue(v interface{}) error {
	ret := _m.Called(v)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(v)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ID provides a mock function with given fields:
func (_m *Message) ID() pulsar.MessageID {
	ret := _m.Called()

	var r0 pulsar.MessageID
	if rf, ok := ret.Get(0).(func() pulsar.MessageID); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pulsar.MessageID)
		}
	}

	return r0
}

// Index provides a mock function with given fields:
func (_m *Message) Index() *uint64 {
	ret := _m.Called()

	var r0 *uint64
	if rf, ok := ret.Get(0).(func() *uint64); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*uint64)
		}
	}

	return r0
}

// IsReplicated provides a mock function with given fields:
func (_m *Message) IsReplicated() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// Key provides a mock function with given fields:
func (_m *Message) Key() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// OrderingKey provides a mock function with given fields:
func (_m *Message) OrderingKey() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// Payload provides a mock function with given fields:
func (_m *Message) Payload() []byte {
	ret := _m.Called()

	var r0 []byte
	if rf, ok := ret.Get(0).(func() []byte); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	return r0
}

// ProducerName provides a mock function with given fields:
func (_m *Message) ProducerName() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// Properties provides a mock function with given fields:
func (_m *Message) Properties() map[string]string {
	ret := _m.Called()

	var r0 map[string]string
	if rf, ok := ret.Get(0).(func() map[string]string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]string)
		}
	}

	return r0
}

// PublishTime provides a mock function with given fields:
func (_m *Message) PublishTime() time.Time {
	ret := _m.Called()

	var r0 time.Time
	if rf, ok := ret.Get(0).(func() time.Time); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Time)
	}

	return r0
}

// RedeliveryCount provides a mock function with given fields:
func (_m *Message) RedeliveryCount() uint32 {
	ret := _m.Called()

	var r0 uint32
	if rf, ok := ret.Get(0).(func() uint32); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint32)
	}

	return r0
}

// SchemaVersion provides a mock function with given fields:
func (_m *Message) SchemaVersion() []byte {
	ret := _m.Called()

	var r0 []byte
	if rf, ok := ret.Get(0).(func() []byte); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	return r0
}

// Topic provides a mock function with given fields:
func (_m *Message) Topic() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

type mockConstructorTestingTNewMessage interface {
	mock.TestingT
	Cleanup(func())
}

// NewMessage creates a new instance of Message. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMessage(t mockConstructorTestingTNewMessage) *Message {
	mock := &Message{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
