## Zookeeper in VoltDB

Simple, lightweight, dependable CLI for ZooKeeper

### Build zookeepercli

zookeepercli is a non-interactive command line client for ZooKeeper.

```bash
$ go get github.com/let-us-go/zkcli
$ go install github.com/let-us-go/zkcli
```

### zookeepercli commands

```bash
$ zkcli -help

get <path>
ls <path>
create <path> [<data>]
set <path> [<data>]
delete <path>
connect <host:port>
addauth <scheme> <auth>
close

Usage of zkcli:
  -p string
        Password
  -s string
        Servers (default "127.0.0.1:2181")
  -u string
        Username
  -version
        Show version info
```



