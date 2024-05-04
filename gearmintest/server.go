// Package gearmintest provides utilities for testing code that uses gearmin.
package gearmintest

import (
	"fmt"

	"github.com/artefactual-labs/gearmin"
	"github.com/mikespook/gearman-go/worker"
	"github.com/phayes/freeport"
)

type testingT interface {
	Helper()
	Cleanup(func())
	Fatalf(format string, args ...any)
}

type Handler func(job worker.Job) ([]byte, error)

// Server returns a started gearmin server ready to perform jobs given an map
// of job handlers indexed by their function names. The server is stopped
// automatically.
func Server(t testingT, handlers map[string]Handler) *gearmin.Server {
	t.Helper()

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to open free port: %v", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	srv := gearmin.NewServer(gearmin.Config{ListenAddr: addr})
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	t.Cleanup(func() { srv.Stop() })

	createWorker(t, addr, handlers)

	return srv
}

func createWorker(t testingT, addr string, handlers map[string]Handler) {
	t.Helper()

	w := worker.New(worker.OneByOne)
	t.Cleanup(func() { w.Close() })

	if err := w.AddServer("tcp", addr); err != nil {
		t.Fatalf("Failed to add server to worker: %v", err)
	}

	for name, handler := range handlers {
		if err := w.AddFunc(name, worker.JobFunc(handler), 0); err != nil {
			t.Fatalf("Failed to add function to worker: %v", err)
		}
	}

	if err := w.Ready(); err != nil {
		t.Fatalf("Failed to show readyness from worker: %v", err)
	}

	go w.Work()
	t.Cleanup(func() {
		w.Close()
	})
}
