# gearmin

<p align="left">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg"/></a>
  <a href="https://codecov.io/gh/artefactual-labs/gearmin"><img src="https://img.shields.io/codecov/c/github/artefactual-labs/gearmin"/></a>
  <a href="https://pkg.go.dev/github.com/artefactual-labs/gearmin"><img src="https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square"/></a>
</p>

**gearmin** is a lightweight and embeddable implementation of the Gearman job
server, written in Go, that partially implements the Gearman protocol. It is
designed for developers looking for an in-process job server compatible with
existing Gearman workers. Unlike [gearmand], job submissions are facilitated
through direct API access.

## Non-goals

We're not planning to support the following features:

- Standalone server mode.
- Persistent queues.
- Job scheduling, timeouts, reducers and priorities.
- Job submission via Gearman clients.

## Usage

```go
srv := gearmin.NewServerWithAddr(":4730")
defer srv.Stop()

srv.Submit(&gearmin.JobRequest{
  FuncName:   "sum",
  Data:       []byte(`{"x":1,"y":2}`),
  Background: false,
  Callback: func(update JobUpdate) {
    fmt.Println("Done!")
  },
})
```

Use the [gearmintest] package to test code that uses gearmin.

## Acknowledgement

* Based on: https://github.com/gearman/gearmand.
* Developed under the foundation of [appscode/g2], [ngaut/gearmand] and
  [mikespook/gearman-go].

## License

Apache 2.0. See [LICENSE](LICENSE).

- Copyright (C) by AppsCode Inc.
- Copyright (C) by @ngaut


[gearmand]: https://github.com/gearman/gearmand
[appscode/g2]: https://github.com/appscode/g2
[ngaut/gearmand]: https://github.com/ngaut/gearmand
[mikespook/gearman-go]: https://github.com/mikespook/gearman-go
[gearmintest]: https://pkg.go.dev/github.com/artefactual-labs/gearmin/gearmintest
