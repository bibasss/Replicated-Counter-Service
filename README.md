# Replicated Counter Service

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install grpcio grpcio-tools pytest
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

## Start one replica

```bash
python3 server.py --port 50051
```

Fault injection: `--delay-ms 3000` delays every Increment reply (the write is
still applied, only the reply is late).

## Client

```bash
python3 client.py incr likes:post-42 --by 5
python3 client.py incr likes:post-42 --by 1 --key my-key   # reuse a key to see dedup
python3 client.py get likes:post-42
python3 client.py --target localhost:50052 get likes:post-42
```
