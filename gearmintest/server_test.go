package gearmintest_test

import (
	"testing"

	"github.com/mikespook/gearman-go/worker"
	"gotest.tools/v3/assert"

	"github.com/artefactual-labs/gearmin"
	"github.com/artefactual-labs/gearmin/gearmintest"
)

func TestServer(t *testing.T) {
	srv := gearmintest.Server(t, map[string]gearmintest.Handler{
		"hello": func(job worker.Job) ([]byte, error) {
			return []byte("hello"), nil
		},
	})

	done := make(chan gearmin.JobUpdate)
	srv.Submit(&gearmin.JobRequest{
		FuncName: "hello",
		Callback: func(update gearmin.JobUpdate) {
			done <- update
		},
	})

	update := <-done
	assert.Equal(t, update.Type, gearmin.JobUpdateTypeComplete)
	assert.DeepEqual(t, update.Data, []byte("hello"))
}
