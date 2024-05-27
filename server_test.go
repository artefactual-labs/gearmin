package gearmin_test

import (
	"errors"
	"net"
	"net/netip"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"
	detectrace "github.com/ipfs/go-detect-race"
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

	// This test is skipped if the data race is enabled because of a known race
	// in the worker package that doesn not belong to this project. For more
	// details, visit: https://gist.github.com/sevein/d7f36e6c5f2721335568905754ffabb3.
	if detectrace.WithRace() {
		t.SkipNow()
	}

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

	// Test CANT_DO.
	w.SetId("new-id")
	w.RemoveFunc("sum")
	w.RemoveFunc("max")
	srv.Submit(&gearmin.JobRequest{
		FuncName:   "sum",
		Data:       []byte(`{ "x": 1, "y": 1 }`),
		Background: false,
		Callback: func(update gearmin.JobUpdate) {
			t.Fatal() // Should not be processed because the func was removed.
		},
	})
	time.Sleep(time.Millisecond * 10) // Give the server some time to run it.

	w.Close()

	srv.Stop()
	srv.Stop() // should not panic.
	srv = nil
	srv.Stop() // should not panic.
}

func TestUpdates(t *testing.T) {
	t.Parallel()

	srv := createServer(t)
	createWorker(t, *srv.Addr(), map[string]worker.JobFunc{
		"say": func(j worker.Job) ([]byte, error) {
			j.SendData([]byte("data"))
			j.SendWarning([]byte("warning"))
			return j.Data(), nil
		},
	})

	updates := make(chan gearmin.JobUpdate, 10)
	srv.Submit(&gearmin.JobRequest{
		FuncName:   "say",
		Data:       []byte("data"),
		Background: false,
		Callback: func(update gearmin.JobUpdate) {
			updates <- update
		},
	})

	results := make([]gearmin.JobUpdate, 0, 3)
	for i := 0; i < 3; i++ {
		if r, ok := <-updates; ok {
			results = append(results, r)
		}
	}

	assert.DeepEqual(t,
		results,
		[]gearmin.JobUpdate{
			{
				Type: gearmin.JobUpdateTypeData,
				Data: []byte("data"),
			},
			{
				Type: gearmin.JobUpdateTypeWarning,
				Data: []byte("warning"),
			},
			{
				Type: gearmin.JobUpdateTypeComplete,
				Data: []byte("data"),
			},
		},
		cmpopts.IgnoreFields(gearmin.JobUpdate{}, "Handle"),
		cmpopts.SortSlices(func(x, y gearmin.JobUpdate) bool { return x.Type > y.Type }),
	)
}

func TestWithGearmanGoWorker(t *testing.T) {
	t.Parallel()

	t.Skip("gearman-go is unreliable, see issue #3 for more details.")

	var wg sync.WaitGroup

	var (
		total          = 1000 // Jobs.
		executed int64 = 0    // Executed by the worker (tasks).
		reported int64 = 0    // Reported by the server (callbacks).
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

func TestHighLoadWithGearmanCLIWorker(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup

	var (
		workers        = 3    // Number of workers to run in parallel.
		total          = 1000 // Jobs.
		reported int64 = 0    // Reported by the server (callbacks).
	)

	srv := createServer(t)
	for i := 0; i < workers; i++ {
		createEchoWorker(t, srv.Addr().AddrPort(), "say")
	}

	for i := 0; i < total; i++ {
		wg.Add(1)
		n := strconv.Itoa(i)
		data := []byte(n)
		srv.Submit(&gearmin.JobRequest{
			ID:         n,
			FuncName:   "say",
			Data:       data,
			Background: false,
			Callback: func(update gearmin.JobUpdate) {
				atomic.AddInt64(&reported, 1)
				assert.Equal(t, update.Succeeded(), true)
				assert.Equal(t, string(update.Data), string(data))
				wg.Done()
			},
		})
	}

	wg.Wait()
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

// createEchoWorker creates a worker using the gearman command.
// It echos the job payload.
func createEchoWorker(t *testing.T, addr netip.AddrPort, fn string) {
	t.Helper()

	gearmanPath, err := exec.LookPath("gearman")
	if err != nil {
		t.Skip("gearman executable not found, install gearman-tools")
	}

	args := []string{"-w", "-h", net.IPv4zero.String(), "-p", strconv.Itoa(int(addr.Port())), "-f", fn, "cat"}
	cmd := exec.Command(gearmanPath, args...)
	t.Logf("Connecting gearman CLI worker to server (%s)...", addr)

	cmd.Start()
	assert.NilError(t, err)

	t.Cleanup(func() {
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Process.Wait()
	})
}
