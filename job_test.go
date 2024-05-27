package gearmin

import (
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestJobUpdateType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateType JobUpdateType
		str        string
		succeeded  bool
		failed     bool
	}{
		{
			JobUpdateTypeData,
			"JobUpdateTypeData",
			false, false,
		},
		{
			JobUpdateTypeWarning,
			"JobUpdateTypeWarning",
			false, false,
		},
		{
			JobUpdateTypeStatus,
			"JobUpdateTypeStatus",
			false, false,
		},
		{
			JobUpdateTypeComplete,
			"JobUpdateTypeComplete",
			true, false,
		},
		{
			JobUpdateTypeFail,
			"JobUpdateTypeFail",
			false, true,
		},
		{
			JobUpdateTypeException,
			"JobUpdateTypeException",
			false, true,
		},
	}
	for index, tc := range tests {
		index, tc := index, tc
		name := strings.TrimPrefix(tc.str, "JobUpdateType")
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, JobUpdateType(index), tc.updateType)
			assert.Equal(t, tc.updateType.String(), tc.str)
		})
	}
}

func TestJobHandleGenerator(t *testing.T) {
	t.Run("Generates unique job handles", func(t *testing.T) {
		patchNowFunc(t)
		patchPidFunc(t)
		patchHostnameFunc(t, func() (string, error) { return "hostname", nil })

		gen := jobHandleGenerator{}

		assert.Equal(t, gen.jobHandle(), "H:hostname:12345-1577840523-1")
		assert.Equal(t, gen.jobHandle(), "H:hostname:12345-1577840523-2")
		assert.Equal(t, gen.jobHandle(), "H:hostname:12345-1577840523-3")
	})

	t.Run("Provides thread-safety", func(t *testing.T) {
		gen := jobHandleGenerator{}

		var wg sync.WaitGroup
		ch := make(chan string)
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ch <- gen.jobHandle()
			}()
		}

		results := []string{}
		for i := 0; i < 10; i++ {
			results = append(results, <-ch)
		}

		wg.Wait()
		assert.Equal(t, len(results), 10)
	})

	t.Run("Handles error in hostname function", func(t *testing.T) {
		patchNowFunc(t)
		patchPidFunc(t)
		patchHostnameFunc(t, func() (string, error) { return "", io.EOF })

		gen := jobHandleGenerator{}

		assert.Equal(t, gen.jobHandle(), "H:localhost:12345-1577840523-1")
		assert.Equal(t, gen.jobHandle(), "H:localhost:12345-1577840523-2")
		assert.Equal(t, gen.jobHandle(), "H:localhost:12345-1577840523-3")
	})
}

func patchHostnameFunc(t *testing.T, fn func() (string, error)) {
	t.Helper()
	hostname = fn
	t.Cleanup(func() {
		hostname = os.Hostname
	})
}

func patchNowFunc(t *testing.T) {
	t.Helper()
	now = func() time.Time { return time.Date(2020, 1, 1, 1, 2, 3, 12345, time.UTC) }
	t.Cleanup(func() {
		now = time.Now
	})
}

func patchPidFunc(t *testing.T) {
	t.Helper()
	pid = func() int { return 12345 }
	t.Cleanup(func() {
		pid = os.Getpid
	})
}
