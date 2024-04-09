package gearmin_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/mikespook/gearman-go/worker"
	"github.com/phayes/freeport"
	"gotest.tools/v3/assert"

	"github.com/sevein/gearmin"
)

func TestIntegration(t *testing.T) {
	addr := fmt.Sprintf("127.0.0.1:%d", freeport.GetPort())
	srv := gearmin.NewServer(
		gearmin.Config{
			ListenAddr: addr,
		},
	)

	err := srv.Start()
	assert.NilError(t, err)

	w := worker.New(worker.Unlimited)
	defer w.Close()
	w.JobHandler = func(job worker.Job) error {
		t.Log(job.Data())
		return nil
	}
	w.AddServer("tcp4", addr)

	var (
		jobDone      = make(chan struct{})
		callbackDone = make(chan struct{})
	)

	w.AddFunc(
		"sum",
		func(j worker.Job) ([]byte, error) {
			jobDone <- struct{}{}
			return []byte(`2`), nil
		},
		worker.Unlimited,
	)
	w.AddFunc(
		"max",
		func(j worker.Job) ([]byte, error) {
			jobDone <- struct{}{}
			return nil, errors.New("unexpected error")
		},
		worker.Unlimited,
	)

	err = w.Ready()
	assert.NilError(t, err)
	go w.Work()
	t.Cleanup(func() { w.Close() })

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

	// TODO: data race.
	time.Sleep(time.Millisecond * 100)

	w.Close()
	srv.Stop()
	srv.Stop() // should not panic.

	srv = nil
	srv.Stop()
}
