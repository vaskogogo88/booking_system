import signal
import sys
import zmq

if len(sys.argv) != 3:
    sys.exit("usage: python client.py <client_id> <client_port>")

client_id = sys.argv[1]
client_port = sys.argv[2]

if ":" in client_id:
    sys.exit(f"error: client_id '{client_id}' must not contain ':'")
if not client_port.isdigit():
    sys.exit(f"error: invalid client port '{client_port}'")

counter = 0
state = "IDLE"
ignored_sigint = False


def on_ctrl_c(signum, frame):
    # Only sets flags; all printing happens in the main loop.
    global state, ignored_sigint
    if state == "IDLE":
        state = "READING"
    else:
        ignored_sigint = True


def check_list_positivity(l):
    quantities = l[1::2]
    try:
        all_pos_int = all(int(q) > 0 for q in quantities)
    except ValueError:
        all_pos_int = False
    return all_pos_int


def report_ignored_sigint():
    global ignored_sigint
    if ignored_sigint:
        print(f"[{client_id}] Ctrl+C ignored: state is {state}")
        ignored_sigint = False


signal.signal(signal.SIGINT, on_ctrl_c)

context = zmq.Context()
client = context.socket(zmq.DEALER)

client.identity = client_id.encode()
client.connect(f"tcp://localhost:{client_port}")

print(f"[{client_id}] ready, press Ctrl+C to send a transaction")

while True:
    report_ignored_sigint()

    if client.poll(500):
        message = client.recv_string()
        parts = message.split()
        if parts[0] == "COMMIT" or parts[0] == "ABORT":
            print(f"[{client_id}] <- coordinator: {message}")
            state = "IDLE"

    if state == "READING":
        while True:
            user_request = input(f"[{client_id}] enter <packetId> <quantity> ...: ")
            report_ignored_sigint()
            parts = user_request.split()

            if len(parts) == 0 or len(parts) % 2 != 0:
                print(f"[{client_id}] input error: expected one or more <packetId> <quantity> pairs")
            elif not check_list_positivity(parts):
                print(f"[{client_id}] input error: all quantities must be positive integers")
            else:
                break

        counter += 1
        transaction_id = f"{client_id}:{counter}"
        request = f"TRANSACTION {transaction_id} {' '.join(parts)}"
        client.send_string(request)
        print(f"[{client_id}] -> coordinator: {request}")
        state = "WAITING"
