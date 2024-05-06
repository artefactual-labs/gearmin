package gearmintest_test

import (
	"testing"

	"gotest.tools/v3/assert"

	"github.com/artefactual-labs/gearmin"
	"github.com/artefactual-labs/gearmin/gearmintest"
	"github.com/artefactual-labs/gearmin/internal/worker"
)

func TestServer(t *testing.T) {
	t.Parallel()

	srv := gearmintest.Server(t, map[string]gearmintest.Handler{
		"hello": func(job worker.Job) ([]byte, error) {
			return []byte("hi!"), nil
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
	assert.DeepEqual(t, update.Data, []byte("hi!"))
}
