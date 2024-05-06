package gearmin_test

import (
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/mikespook/gearman-go/worker"
	"gotest.tools/v3/assert"

	"github.com/artefactual-labs/gearmin"
)

func TestIntegration(t *testing.T) {
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
