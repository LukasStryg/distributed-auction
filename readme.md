# Assignment 5: Distributed Auction

### How to run

All commands are run from the project root.

Start each node in its own terminal, with ids 0, 1 and 2. Start node 2 last, the auction starts when node 2 starts and ends 100 seconds later:

```
go run ./node 0
go run ./node 1
go run ./node 2
```

The nodes find each other through hardcoded addresses, node `i` listens on `localhost:500i`. Node 2 starts as the primary, node 0 and 1 are backups.

Start each client in its own terminal, with a unique name and the id of the node it should talk to first:

```
go run ./client Alice 0
go run ./client Bob 2
```

In a client:
- Type `bid <amount>` to bid, e.g. `bid 100`. The client prints `SUCCESS`, `FAIL` or `EXCEPTION`.
- Type `result` to get the highest bid, or the winner if the auction is over.

To crash a node, press Ctrl+C in its terminal. The system keeps working with one crashed node. A crashed node should not be started again.

### Logs

Each node logs to its terminal and to `node<id>.log` in the project root. Each client logs to its terminal and to `<name>.log`.

Logs from an example run with three nodes and two clients are in the logs directory.
