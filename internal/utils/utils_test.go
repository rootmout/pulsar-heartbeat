package utils

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_EqualByteSlice_Equal(t *testing.T) {
	msgA := []byte("text")
	msgB := []byte("text")
	require.True(t, EqualByteSlice(msgA, msgB))
}

func Test_EqualByteSlice_NotEqual(t *testing.T) {
	msgA := []byte("book")
	msgB := []byte("text")
	require.False(t, EqualByteSlice(msgA, msgB))
}
