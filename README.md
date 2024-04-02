# gearmin

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/sevein/gearmin) 


A lightweight, embeddable implementation of the Gearman job server protocol,
written in Go. It is designed for developers looking for an in-process job
server compatible with existing Gearman workers. Unlike [gearmand], job
submissions are facilitated through direct API access.

**Please do not use this implementation yet as it has not undergone testing.**

## Usage

```go
srv := gearmin.NewServer(
  gearmin.Config{
    ListenAddr: fmt.Sprintf(":%d", freeport.GetPort()),
  },
)

_ = srv.Start()
defer srv.Stop()

srv.Submit(&JobRequest{...})
```

## Acknowledgement

* Package forked from: https://github.com/appscode/g2
* Gearman project: https://github.com/gearman/gearmand

## License

Apache 2.0. See [LICENSE](LICENSE).

- Copyright (C) by AppsCode Inc.
- Copyright (C) by @ngaut


[gearmand]: (https://github.com/gearman/gearmand/tree/master)
