# gearmin

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/artefactual-labs/gearmin)


A lightweight, embeddable implementation of the Gearman job server protocol,
written in Go. It is designed for developers looking for an in-process job
server compatible with existing Gearman workers. Unlike [gearmand], job
submissions are facilitated through direct API access.

**Please do not use this implementation yet as it has not undergone testing.**

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

## Acknowledgement

* Based on: https://github.com/gearman/gearmand.
* Developed under the foundation of [appscode/g2] and [ngaut/gearmand].

## License

Apache 2.0. See [LICENSE](LICENSE).

- Copyright (C) by AppsCode Inc.
- Copyright (C) by @ngaut


[gearmand]: https://github.com/gearman/gearmand
[appscode/g2]: https://github.com/appscode/g2
[ngaut/gearmand]: https://github.com/ngaut/gearmand
