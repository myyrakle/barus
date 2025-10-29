# barus

![](https://img.shields.io/badge/language-Rust-red) ![](https://img.shields.io/badge/version-0.0.2-brightgreen) [![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/myyrakle/barus/blob/master/LICENSE)

- lightweight key/value storage

## Get Started

The simplest way to start a server is to use Docker.

```bash
sudo docker run -p 53000:53000 -p 53001:53001 myyrakle/barus:v0.0.2
```

```bash
curl http://localhost:53000/status

# create new table
curl -X POST -H "Content-Type: application/json" -d '{}' http://localhost:53000/tables/foo

# insert new value
curl -X PUT -H "Content-Type: application/json" -d '{"key":"1111","value":"1234"}' http://localhost:53000/tables/foo/value

# get value
curl -X GET -H "Content-Type: application/json" http://localhost:53000/tables/foo/value?key=1111

# delete value
curl -X DELETE -H "Content-Type: application/json" http://localhost:53000/tables/foo/value?key=1111
```

## Configuration

- env:BARUS_HTTP_PORT = HTTP server port (default value: 53000)
- env:BARUS_GRPC_PORT = gRPC server port (default value: 53001)
- env:BARUS_DATA_DIR = database base directory (default value: "data")
- env:RUST_LOG = log level (default value: info)
- env:RUST_BACKTRACE = backtrace enable flag. 1=enabled, 0=disabled. (default value: 1)
