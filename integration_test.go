package gearmin_test

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"
	"gotest.tools/v3/assert"

	"github.com/artefactual-labs/gearmin"
	"github.com/artefactual-labs/gearmin/internal/worker"
)

func TestIntegration(t *testing.T) {
	t.Parallel()

	srv := createServer(t)

	var (
		jobDone      = make(chan struct{})
		callbackDone = make(chan struct{})
	)

	w := createWorker(t, srv.Addr(), map[string]worker.JobFunc{
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

func TestWithGoWorker(t *testing.T) {
	var wg sync.WaitGroup

	var (
		total          = 100 // Jobs.
		executed int64 = 0   // Executed by the worker (tasks).
		reported int64 = 0   // Reported by the server (callbacks).
	)

	srv := createAll(t, map[string]worker.JobFunc{
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

func TestWithPythonWorker(t *testing.T) {
	var wg sync.WaitGroup

	var (
		total          = 100 // Jobs.
		reported int64 = 0   // Reported by the server (callbacks).
	)

	srv := createAll2(t)

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

	assert.Equal(t, reported, int64(total))
}

func createAll(t *testing.T, handlers map[string]worker.JobFunc) *gearmin.Server {
	srv := createServer(t)
	_ = createWorker(t, srv.Addr(), handlers)

	return srv
}

func createServer(t *testing.T) *gearmin.Server {
	t.Helper()

	srv, err := gearmin.NewServerWithAddr(":0")
	assert.NilError(t, err)

	t.Cleanup(func() { srv.Stop() })

	return srv
}

func createWorker(t *testing.T, addr *net.TCPAddr, handlers map[string]worker.JobFunc) *worker.Worker {
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

func createAll2(t *testing.T) *gearmin.Server {
	srv := createServer(t)

	addr := fmt.Sprintf("%s:%d", "127.0.0.1", srv.Addr().Port)

	// Download from https://gist.github.com/sevein/751e1c6705e4007723c059c90a072e8c#file-worker-py.
	cmd := exec.Command("/home/jesus/Projects/gearman-python-say-worker/worker.py", addr)

	stdout, err := cmd.StdoutPipe()
	assert.NilError(t, err)

	stderr, err := cmd.StderrPipe()
	assert.NilError(t, err)

	err = cmd.Start()
	assert.NilError(t, err)

	go readOutput(t, stdout, "stdout")
	go readOutput(t, stderr, "stderr")

	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		cmd.Wait()
	})

	return srv
}

// readOutput reads from the given reader and logs the output with the specified label
func readOutput(t *testing.T, r io.Reader, label string) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		t.Logf("%s: %s", label, scanner.Text())
	}
}
