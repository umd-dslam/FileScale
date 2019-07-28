# NNProxy

NNProxy is a reliable, scalable, dynamic proxy that acts as an intermediary for requests from clients seeking specified resources/files from multiple NameNodes.

## Features

- NameNode RPC proxy instead of ViewFS, no client-side changes.
- Multiple language client (i.e. snakebite) support.
- Stateless. Easy to scale to multiple instances.
- High throughput with little latency introduced.
- Request throttle. Single NameNode malfunction will not affect globally.
- Dynamic subtree partioning. Hotspots will be shunted to multiple NameNodes.

## Acknowledgement

We learnt a lot from ByteDance's [HDFS Federation solution](https://github.com/bytedance/nnproxy) when building NNProxy.

