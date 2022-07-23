package latency

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_CheckLatency_Success(t *testing.T) {
	latency, err := CheckLatency(context.Background(), nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), latency)
}
