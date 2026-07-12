# example

The Haskell and Python examples share the same queue: jobs enqueued in one can be consumed by the other.

Run the Haskell example modes:

```sh
./run-example.sh simple
./run-example.sh hpcscheduler
./run-example.sh python
```

Run the Python side or a cross-language demo:

```sh
./run-example.sh py-consumer
./run-example.sh py-producer
./run-example.sh h2p
./run-example.sh p2h
```

`h2p` runs the Haskell producer against the Python consumer. `p2h` runs the Python producer against the Haskell simple example.
