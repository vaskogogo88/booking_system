# Report: Distributed booking system with Two-Phase Commit

## 1. Implementation

The system extends the lab implementation of Two-Phase Commit. The topology is
unchanged: clients connect with a DEALER socket to the coordinator's ROUTER, and the
coordinator holds one DEALER per participant, each connected to that participant's ROUTER.
All processes are single-threaded and use `zmq.Poller` / `Socket.poll()` with a finite
timeout, so they keep serving messages while transactions are pending. Messages are plain
strings parsed with `split()`; state is kept in lists and dictionaries.

### Client (`client.py`)

- States `IDLE`, `READING` and `WAITING`. The SIGINT handler only sets flags: in `IDLE` it
  moves to `READING`; in any other state it sets `ignored_sigint`, and the main loop prints
  the "Ctrl+C ignored" message.
- In `READING` the client reads one line with `input()`, checks that it consists of one or
  more complete `<packetId> <quantity>` pairs and that every quantity is a positive integer.
  On error it prints the reason and asks again, without sending and without increasing the
  transaction counter. Whether the product IDs exist is left to the coordinator.
- A valid line gets the next `transactionId` (`<clientId>:1`, `<clientId>:2`, ...), is sent as
  `TRANSACTION <transactionId> <packetId1> <quantity1> ...`, and the client moves to
  `WAITING` until it receives `COMMIT` or `ABORT`, then returns to `IDLE`.

### Coordinator (`coordinator.py`)

- The static catalog `CATALOG` maps every `packetId` to a list of
  `(participantId, productId, units)` triples. A simple product has one triple with
  `packetId = productId`; a package has several. The coordinator does not know the stock.
- On `TRANSACTION`, it first checks that every `packetId` is in the catalog. An unknown
  product causes an immediate `ABORT` to the client, with no message to any participant.
- For a valid basket, `aggregate_demands` multiplies the units of each component by the
  requested quantity and sums them into `{participantId: {productId: quantity}}`. One
  `PREPARE` is sent per selected participant, listing each of its products once.
- Each basket has its own record in `pending`: client identity, aggregated demands,
  selected participants, vote counters, decision and ACK counter. The coordinator waits for
  one vote per selected participant, decides `COMMIT` only if all of them are `PREPARED`,
  and sends the decision to every selected participant (including those that voted
  `ABORT`). After one ACK per selected participant it replies to the client and deletes the
  record. Transactions with a single participant follow the same path.

### Participant (`participant.py`)

- Keeps `stock[productId]` from its startup arguments and `reservations[transactionId]` as
  a `{productId: quantity}` dictionary. For each product,
  `available = stock − sum of the product's reservations over all pending transactions`.
- On `PREPARE` it first checks every requested product without changing anything. If all
  fit, it reserves all quantities and decides `PREPARED`; otherwise it decides `ABORT` and
  reserves nothing. Check and reservation happen in one step, before any other message is
  served.
- On `COMMIT` it reduces the stock by the reserved quantities and removes the reservation.
  On `ABORT` it removes the reservation if there is one and never increases the stock. In
  both cases it replies with `ACK`, even if it had voted `ABORT`.
- Vote delay: once the vote is decided it is stored with
  `send_at = time.monotonic() + delay_ms / 1000`. At the start of every loop iteration all
  pending votes with `time.monotonic() >= send_at` are sent. The vote is never recalculated,
  positive reservations stay in place until the decision, and `time.sleep()` is not used.
  The poll timeout is 10 ms, so votes leave close to their scheduled time. ACKs are not
  delayed.

### Guarantees

Stock can never become negative and products are never over-reserved: a reservation is
only created when it fits into the available quantity, which already accounts for every
other pending reservation, and a `COMMIT` only consumes quantities that were reserved for
that transaction. Reservations of a finished transaction are always removed.

## 2. Test runs

All runs used real processes on the ports from the specification, the startup commands
from the README and the initial stock from the specification. Clients were driven by
sending real SIGINT signals and typing basket lines. The excerpts below are copied from the
logs; lines typed by the user are marked with `⟵ typed`.

Run 1 used `delay_ms = 0` for every participant. Run 2 restarted all processes with
`delay_ms = 3000` for P1.

### Summary

| # | Client input | Expected | Actual |
|---|---|---|---|
| S1a | `TRIP_A` | input error (incomplete pair), nothing sent | ✅ as expected |
| S1b | `TRIP_A 0` | input error (not positive), nothing sent | ✅ as expected |
| S1c | `TRIP_A two` | input error (not an integer), nothing sent | ✅ as expected |
| S1d | Ctrl+C while reading | ignored with a message | ✅ as expected |
| S1 | `TRIP_A 2 HOTEL_A 1 HOTEL_B 1` | P0: `FLIGHT_A 2`, P1: `HOTEL_A 3 HOTEL_B 1`, `COMMIT` | ✅ `COMMIT C0:1` |
| S2 | `TRIP_X 1 HOTEL_A 1` | `ABORT` without contacting participants | ✅ `ABORT C1:1` |
| S3 | `TRIP_FULL 2` | P1 has only 1 `HOTEL_A` → `ABORT`, P0 and P2 release their reservations | ✅ `ABORT C1:2` |
| S4 | `FLIGHT_A 3 TOUR_A 6` | `COMMIT`, uses the stock released in S3 | ✅ `COMMIT C0:2` |
| S5a | `TOUR_B 3` | single participant, `COMMIT`, `TOUR_B` → 0 | ✅ `COMMIT C1:3` |
| S5b | `TOUR_B 1` | no stock left → `ABORT` | ✅ `ABORT C1:4` |
| S6 | C0 `HOTEL_A 3`, then C1 `HOTEL_A 2` (P1 delay 3000 ms) | C1's PREPARE is served during the delay and sees only 1 available → C0 `COMMIT`, C1 `ABORT` | ✅ `COMMIT C0:1`, `ABORT C1:1` |
| S6b | Ctrl+C on C0 while waiting | ignored with a message | ✅ as expected |
| S7 | C1 `HOTEL_A 1` | the last `HOTEL_A` → `COMMIT`, stock 0 | ✅ `COMMIT C1:2` |

Final stock after run 1: `FLIGHT_A 0, FLIGHT_B 3, HOTEL_A 1, HOTEL_B 1, TOUR_A 0, TOUR_B 0`,
which matches the committed transactions (S1, S4, S5a).

### S1: input validation and the basket from the specification

Client C0:

```
[C0] ready, press Ctrl+C to send a transaction
[C0] Ctrl+C ignored: state is READING
TRIP_A                                                         ⟵ typed
[C0] input error: expected one or more <packetId> <quantity> pairs
TRIP_A 0                                                       ⟵ typed
[C0] input error: all quantities must be positive integers
TRIP_A two                                                     ⟵ typed
[C0] input error: all quantities must be positive integers
TRIP_A 2 HOTEL_A 1 HOTEL_B 1                                   ⟵ typed
[C0] -> coordinator: TRANSACTION C0:1 TRIP_A 2 HOTEL_A 1 HOTEL_B 1
[C0] <- coordinator: COMMIT C0:1
```

The three invalid lines did not use up a transaction number: the first transaction is
`C0:1`.

Coordinator. The aggregation matches the example of the specification (`HOTEL_A`: 2 from
`TRIP_A` plus 1 requested directly):

```
[COORD] <- client C0: TRANSACTION C0:1 TRIP_A 2 HOTEL_A 1 HOTEL_B 1
[COORD] aggregated demands for C0:1:
[COORD]   P0: {'FLIGHT_A': 2}
[COORD]   P1: {'HOTEL_A': 3, 'HOTEL_B': 1}
[COORD] -> participant P0: PREPARE C0:1 FLIGHT_A 2
[COORD] -> participant P1: PREPARE C0:1 HOTEL_A 3 HOTEL_B 1
[COORD] <- P0: PREPARED C0:1
[COORD] C0:1: votes 1/2 (1 PREPARED)
[COORD] <- P1: PREPARED C0:1
[COORD] C0:1: votes 2/2 (2 PREPARED)
[COORD] decision for C0:1: COMMIT (2/2 PREPARED)
[COORD] -> P0: COMMIT C0:1
[COORD] -> P1: COMMIT C0:1
[COORD] <- P0: ACK C0:1
[COORD] C0:1: ACKs 1/2
[COORD] <- P1: ACK C0:1
[COORD] C0:1: ACKs 2/2
[COORD] -> client: COMMIT C0:1
```

P2 is not involved and receives nothing. Participant P1:

```
[P1] <- coordinator: PREPARE C0:1 HOTEL_A 3 HOTEL_B 1
[P1]   before HOTEL_A: stock=4 reserved=0 available=4
[P1]   before HOTEL_B: stock=2 reserved=0 available=2
[P1] C0:1: enough stock, reserved {'HOTEL_A': 3, 'HOTEL_B': 1}
[P1]   after  HOTEL_A: stock=4 reserved=3 available=1
[P1]   after  HOTEL_B: stock=2 reserved=1 available=1
[P1] scheduled PREPARED C0:1 P1 (delay 0 ms)
[P1] -> coordinator: PREPARED C0:1 P1 (sent after delay)
[P1] <- coordinator: COMMIT C0:1
[P1] decision for C0:1: COMMIT
[P1]   before HOTEL_A: stock=4 reserved=3 available=1
[P1]   before HOTEL_B: stock=2 reserved=1 available=1
[P1]   after  HOTEL_A: stock=1 reserved=0 available=1
[P1]   after  HOTEL_B: stock=1 reserved=0 available=1
[P1] -> coordinator: ACK C0:1 P1
```

### S2: unknown product

```
[C1] -> coordinator: TRANSACTION C1:1 TRIP_X 1 HOTEL_A 1
[C1] <- coordinator: ABORT C1:1
```
```
[COORD] <- client C1: TRANSACTION C1:1 TRIP_X 1 HOTEL_A 1
[COORD] unknown product in C1:1, -> client: ABORT C1:1
```

No participant log contains `C1:1`: the basket is rejected before any `PREPARE`, even though
`HOTEL_A` itself is valid.

### S3: one participant cannot serve a package

`TRIP_FULL 2` needs 2 × `HOTEL_A`, but after S1 only 1 is left.

```
[COORD] aggregated demands for C1:2:
[COORD]   P0: {'FLIGHT_A': 2}
[COORD]   P1: {'HOTEL_A': 2}
[COORD]   P2: {'TOUR_A': 2}
[COORD] <- P0: PREPARED C1:2
[COORD] C1:2: votes 1/3 (1 PREPARED)
[COORD] <- P1: ABORT C1:2
[COORD] C1:2: votes 2/3 (1 PREPARED)
[COORD] <- P2: PREPARED C1:2
[COORD] C1:2: votes 3/3 (2 PREPARED)
[COORD] decision for C1:2: ABORT (2/3 PREPARED)
[COORD] -> P0: ABORT C1:2
[COORD] -> P1: ABORT C1:2
[COORD] -> P2: ABORT C1:2
```

P1 votes `ABORT` without reserving anything, still receives the decision and sends `ACK`:

```
[P1] <- coordinator: PREPARE C1:2 HOTEL_A 2
[P1]   before HOTEL_A: stock=1 reserved=0 available=1
[P1] C1:2: not enough stock, nothing reserved
[P1]   after  HOTEL_A: stock=1 reserved=0 available=1
[P1] -> coordinator: ABORT C1:2 P1 (sent after delay)
[P1] <- coordinator: ABORT C1:2
[P1] -> coordinator: ACK C1:2 P1
```

P0 had reserved and releases the reservation on `ABORT` without changing the stock (P2
behaves the same way for `TOUR_A`):

```
[P0] C1:2: enough stock, reserved {'FLIGHT_A': 2}
[P0]   after  FLIGHT_A: stock=3 reserved=2 available=1
[P0] <- coordinator: ABORT C1:2
[P0]   before FLIGHT_A: stock=3 reserved=2 available=1
[P0]   after  FLIGHT_A: stock=3 reserved=0 available=3
[P0] -> coordinator: ACK C1:2 P0
```

### S4: the released stock is available again

`FLIGHT_A 3 TOUR_A 6` only succeeds if the reservations of S3 were really removed. It
commits and takes both products to 0:

```
[P0]   before FLIGHT_A: stock=3 reserved=0 available=3
[P0] C0:2: enough stock, reserved {'FLIGHT_A': 3}
...
[P0]   after  FLIGHT_A: stock=0 reserved=0 available=0
```
```
[P2]   before TOUR_A: stock=6 reserved=0 available=6
[P2] C0:2: enough stock, reserved {'TOUR_A': 6}
...
[P2]   after  TOUR_A: stock=0 reserved=0 available=0
```

### S5: single-participant transactions down to zero stock

```
[COORD] aggregated demands for C1:3:
[COORD]   P2: {'TOUR_B': 3}
[COORD] C1:3: votes 1/1 (1 PREPARED)
[COORD] decision for C1:3: COMMIT (1/1 PREPARED)
[COORD] C1:3: ACKs 1/1
[COORD] -> client: COMMIT C1:3
...
[COORD] C1:4: votes 1/1 (0 PREPARED)
[COORD] decision for C1:4: ABORT (0/1 PREPARED)
[COORD] -> client: ABORT C1:4
```
```
[P2] <- coordinator: PREPARE C1:4 TOUR_B 1
[P2]   before TOUR_B: stock=0 reserved=0 available=0
[P2] C1:4: not enough stock, nothing reserved
```

### S6: overlapping transactions with a delayed vote (run 2, P1 `delay_ms = 3000`)

C0 sends `HOTEL_A 3`; about 0.5 s later, while P1's vote for `C0:1` is still waiting, C1
sends `HOTEL_A 2`. C0 also presses Ctrl+C while waiting.

```
[C0] -> coordinator: TRANSACTION C0:1 HOTEL_A 3
[C0] Ctrl+C ignored: state is WAITING
[C0] <- coordinator: COMMIT C0:1
```
```
[C1] -> coordinator: TRANSACTION C1:1 HOTEL_A 2
[C1] <- coordinator: ABORT C1:1
```

P1 serves the second `PREPARE` during the delay. The reservation of `C0:1` already holds 3
of the 4 rooms, so `C1:1` sees only 1 available and is refused. Both votes are sent about 3 s
after being scheduled, and the `ACK` for `C0:1` is sent immediately:

```
[P1] <- coordinator: PREPARE C0:1 HOTEL_A 3
[P1]   before HOTEL_A: stock=4 reserved=0 available=4
[P1] C0:1: enough stock, reserved {'HOTEL_A': 3}
[P1]   after  HOTEL_A: stock=4 reserved=3 available=1
[P1] scheduled PREPARED C0:1 P1 (delay 3000 ms)
[P1] <- coordinator: PREPARE C1:1 HOTEL_A 2
[P1]   before HOTEL_A: stock=4 reserved=3 available=1
[P1] C1:1: not enough stock, nothing reserved
[P1]   after  HOTEL_A: stock=4 reserved=3 available=1
[P1] scheduled ABORT C1:1 P1 (delay 3000 ms)
[P1] -> coordinator: PREPARED C0:1 P1 (sent after delay)
[P1] <- coordinator: COMMIT C0:1
[P1]   before HOTEL_A: stock=4 reserved=3 available=1
[P1]   after  HOTEL_A: stock=1 reserved=0 available=1
[P1] -> coordinator: ACK C0:1 P1
[P1] -> coordinator: ABORT C1:1 P1 (sent after delay)
[P1] <- coordinator: ABORT C1:1
[P1] -> coordinator: ACK C1:1 P1
```

The coordinator keeps the two transactions in separate records and handles them
independently while both are pending:

```
[COORD] -> participant P1: PREPARE C0:1 HOTEL_A 3
[COORD] <- client C1: TRANSACTION C1:1 HOTEL_A 2
[COORD] -> participant P1: PREPARE C1:1 HOTEL_A 2
[COORD] <- P1: PREPARED C0:1
[COORD] decision for C0:1: COMMIT (1/1 PREPARED)
...
[COORD] -> client: COMMIT C0:1
[COORD] <- P1: ABORT C1:1
[COORD] decision for C1:1: ABORT (0/1 PREPARED)
...
[COORD] -> client: ABORT C1:1
```

### S7: retry with the remaining stock

```
[C1] -> coordinator: TRANSACTION C1:2 HOTEL_A 1
[C1] <- coordinator: COMMIT C1:2
```
```
[P1]   before HOTEL_A: stock=1 reserved=0 available=1
[P1] C1:2: enough stock, reserved {'HOTEL_A': 1}
...
[P1]   after  HOTEL_A: stock=0 reserved=0 available=0
```

Exactly 4 `HOTEL_A` rooms were sold in total (3 + 1) out of an initial stock of 4: no
overbooking, and the stock never became negative.
