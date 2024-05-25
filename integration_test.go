package gearmin_test

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/mikespook/gearman-go/worker"
	"go.uber.org/goleak"
	"gotest.tools/v3/assert"

	"github.com/artefactual-labs/gearmin"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestServer(t *testing.T) {
	t.Parallel()

	srv := createServer(t)

	var (
		jobDone      = make(chan struct{})
		callbackDone = make(chan struct{})
	)

	w := createWorker(t, *srv.Addr(), map[string]worker.JobFunc{
		"sum": func(j worker.Job) ([]byte, error) {
			jobDone <- struct{}{}
			return []byte(`2`), nil
		},
		"max": func(j worker.Job) ([]byte, error) {
			jobDone <- struct{}{}
			return nil, errors.New("unexpected error")
		},
	})

	jobHandle := srv.Submit(&gearmin.JobRequest{
		FuncName:   "max",
		Data:       []byte(`{ "x": 1, "y": 1 }`),
		Background: true,
	})
	assert.Assert(t, strings.HasPrefix(jobHandle, "H:"))

	jobHandle = srv.Submit(&gearmin.JobRequest{
		FuncName:   "sum",
		Data:       []byte(`{ "x": 1, "y": 1 }`),
		Background: false,
		Callback: func(update gearmin.JobUpdate) {
			assert.Equal(t, update.Failed(), false)
			assert.Equal(t, update.Succeeded(), true)
			assert.DeepEqual(t,
				update, gearmin.JobUpdate{
					Type: gearmin.JobUpdateTypeComplete,
					Data: []byte("2"),
				},
				cmpopts.IgnoreFields(gearmin.JobUpdate{}, "Handle"),
			)
			callbackDone <- struct{}{}
		},
	})
	assert.Assert(t, strings.HasPrefix(jobHandle, "H:"))

	<-jobDone
	<-jobDone
	<-callbackDone

	w.RemoveFunc("sum")
	w.RemoveFunc("max")

	w.Close()

	srv.Stop()
	srv.Stop() // should not panic.
	srv = nil
	srv.Stop() // should not panic.
}

func TestHighLoad(t *testing.T) {
	// Reproduce with: go test -v -run=TestHighLoad -count=10 -timeout=10s
	t.Skip("This fails, see issue #3 for more details.")

	var wg sync.WaitGroup

	var (
		total          = 100 // Jobs.
		executed int64 = 0   // Executed by the worker (tasks).
		reported int64 = 0   // Reported by the server (callbacks).
	)

	srv := createServer(t)
	_ = createWorker(t, *srv.Addr(), map[string]worker.JobFunc{
		"say": func(j worker.Job) ([]byte, error) {
			atomic.AddInt64(&executed, 1)
			return j.Data(), nil
		},
	})

	for i := 0; i < total; i++ {
		wg.Add(1)
		n := strconv.Itoa(i)
		srv.Submit(&gearmin.JobRequest{
			ID:         n,
			FuncName:   "say",
			Data:       []byte(n),
			Background: false,
			Callback: func(update gearmin.JobUpdate) {
				atomic.AddInt64(&reported, 1)
				assert.Equal(t, update.Succeeded(), true)
				assert.Equal(t, string(update.Data), n)
				wg.Done()
			},
		})
	}

	wg.Wait()

	assert.Equal(t, executed, int64(total))
	assert.Equal(t, reported, int64(total))
}

func createServer(t *testing.T) *gearmin.Server {
	t.Helper()

	srv, err := gearmin.NewServerWithAddr(":0")
	assert.NilError(t, err)

	t.Cleanup(func() { srv.Stop() })

	return srv
}

func createWorker(t *testing.T, addr net.TCPAddr, handlers map[string]worker.JobFunc) *worker.Worker {
	t.Helper()

	w := worker.New(worker.OneByOne)
	t.Cleanup(func() { w.Close() })

	w.AddServer(addr.Network(), addr.String())

	for fn, handler := range handlers {
		w.AddFunc(fn, handler, 0)
	}

	err := w.Ready()
	assert.NilError(t, err)

	go w.Work()
	t.Cleanup(func() { w.Close() })

	return w
}
