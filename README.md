# booking_system

A distributed travel booking system built on the Two-Phase Commit (2PC) protocol.
Each client submits a basket of simple products and/or packages; the basket is one
transaction, so either every booking in it succeeds or none does.

- `client.py`: reads baskets interactively and sends them to the coordinator.
- `coordinator.py`: validates and expands each basket, and runs 2PC with only the participants that own the requested products.
- `participant.py`: holds the stock of one service and votes based on stock and temporary reservations.

## Dependencies

- Python 3
- [pyzmq](https://pypi.org/project/pyzmq/)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pyzmq
```

Activate the virtual environment (`source .venv/bin/activate`) in every terminal before
starting a process.

## Starting the system

Start each command in a separate terminal, in this order: participants first, then the
coordinator, then the clients.

```bash
python participant.py P0 6001 0 FLIGHT_A:5 FLIGHT_B:3
python participant.py P1 6002 0 HOTEL_A:4 HOTEL_B:2
python participant.py P2 6003 0 TOUR_A:6 TOUR_B:3
python coordinator.py 6000 P0:6001 P1:6002 P2:6003
python client.py C0 6000
python client.py C1 6000
```

### Arguments

```
python participant.py <id> <port> <delay_ms> <product>:<stock> ...
python coordinator.py <client_port> <participant_id>:<port> ...
python client.py <client_id> <client_port>
```

- `delay_ms`: how many milliseconds the participant waits before sending each vote
  (`PREPARED`/`ABORT`). The vote and any reservation are decided immediately; only the
  sending is delayed. Use `0` for no delay. ACKs are never delayed.
- The participants and products given at startup must match the coordinator's catalog below.
- Every running client must have a unique `client_id`, which must not contain `:`.

## Products and packages

| Participant / port | Service | Products (initial stock) |
|---|---|---|
| P0 / 6001 | Flights | `FLIGHT_A` (5), `FLIGHT_B` (3) |
| P1 / 6002 | Hotels | `HOTEL_A` (4), `HOTEL_B` (2) |
| P2 / 6003 | Tours | `TOUR_A` (6), `TOUR_B` (3) |

| Package | Contents per unit |
|---|---|
| `TRIP_A` | `FLIGHT_A` × 1, `HOTEL_A` × 1 |
| `TRIP_B` | `FLIGHT_B` × 1, `HOTEL_B` × 1 |
| `TRIP_FULL` | `FLIGHT_A` × 1, `HOTEL_A` × 1, `TOUR_A` × 1 |

## Using the client

1. Press **Ctrl+C** in the client's terminal. The client asks for a basket.
2. Type one line of `<packetId> <quantity>` pairs and press Enter, for example:
   ```
   TRIP_A 2 HOTEL_A 1 HOTEL_B 1
   ```
   Quantities must be positive integers. If the line is invalid, the client prints an
   error and asks again; no transaction number is used up.
3. The client sends the transaction (`C0:1`, `C0:2`, ...) and waits. When the result
   arrives it prints `COMMIT <transactionId>` or `ABORT <transactionId>` and is ready for
   the next Ctrl+C.

Ctrl+C is ignored, with a message, while the client is reading a basket or waiting for a
result. If you press it while the prompt is open, the message appears after you press
Enter, because the client is blocked on `input()` until then.

Unknown product IDs are rejected by the coordinator with `ABORT`, without contacting any
participant.

To stop the participants or the coordinator, press Ctrl+C. To stop a client, press Ctrl+\
or close its terminal, because Ctrl+C is used by the client to start a new basket.
