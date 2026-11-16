import sys
import time
import zmq


def get_stock_dictionary(args):
    stock_dict = {}
    for arg in args:
        parts = arg.split(":")
        if len(parts) != 2:
            sys.exit(f"[{participant_id}] error: '{arg}' must have the form <product>:<stock>")
        product_id, product_stock = parts
        if product_id in stock_dict:
            sys.exit(f"[{participant_id}] error: duplicate product '{product_id}'")
        stock_dict[product_id] = int(product_stock)

    return stock_dict


def check_availability(products_dict, stock, reservations):
    # True only if every requested product has enough available stock.
    # available = stock - sum of reservations of that product across all pending transactions.
    for product_id, quantity in products_dict.items():
        reserved = 0
        for reserved_products in reservations.values():
            reserved += reserved_products.get(product_id, 0)

        available = stock[product_id] - reserved
        if available - quantity < 0:
            return False
    return True


def get_requested_products_dict(products):
    product_ids = products[::2]
    quantities = products[1::2]
    products_dict = {}
    for i in range(len(product_ids)):
        products_dict[product_ids[i]] = int(quantities[i])
    return products_dict


def print_state(label, product_ids, stock, reservations):
    # Prints stock / reserved / available for the given products.
    for product_id in product_ids:
        reserved = 0
        for reserved_products in reservations.values():
            reserved += reserved_products.get(product_id, 0)
        print(f"[{participant_id}]   {label} {product_id}: stock={stock[product_id]} "
              f"reserved={reserved} available={stock[product_id] - reserved}")


participant_id = sys.argv[1]
participant_port = sys.argv[2]
delay_ms = int(sys.argv[3])
products = sys.argv[4:]

context = zmq.Context()
participant = context.socket(zmq.ROUTER)
poller = zmq.Poller()
poller.register(participant, zmq.POLLIN)

participant.identity = participant_id.encode()
participant.bind(f"tcp://*:{participant_port}")

stock = get_stock_dictionary(products)
print(f"[{participant_id}] initial stock: {stock}")

reservations = {}
pending_votes = []   # each entry: [send_at, identity, response]

while True:
    events = dict(poller.poll(10))

    # Send every scheduled vote whose delay has expired.
    now = time.monotonic()
    still_waiting = []
    for send_at, vote_identity, vote_response in pending_votes:
        if now >= send_at:
            participant.send_multipart([vote_identity, vote_response.encode()])
            print(f"[{participant_id}] -> coordinator: {vote_response} (sent after delay)")
        else:
            still_waiting.append([send_at, vote_identity, vote_response])
    pending_votes = still_waiting

    if participant in events:
        # The message is in bytes.
        identity, message = participant.recv_multipart()

        message = message.decode()
        parts = message.split()
        print(f"[{participant_id}] <- coordinator: {message}")

        if parts[0] == "PREPARE":
            transaction_id = parts[1]
            products_dict = get_requested_products_dict(parts[2:])
            print_state("before", products_dict.keys(), stock, reservations)
            if check_availability(products_dict, stock, reservations):
                reservations[transaction_id] = products_dict
                vote = "PREPARED"
                print(f"[{participant_id}] {transaction_id}: enough stock, reserved {products_dict}")
            else:
                vote = "ABORT"
                print(f"[{participant_id}] {transaction_id}: not enough stock, nothing reserved")
            print_state("after ", products_dict.keys(), stock, reservations)

            response = f"{vote} {transaction_id} {participant_id}"
            send_at = time.monotonic() + delay_ms / 1000
            pending_votes.append([send_at, identity, response])
            print(f"[{participant_id}] scheduled {response} (delay {delay_ms} ms)")

        elif parts[0] in ("COMMIT", "ABORT"):
            transaction_id = parts[1]
            print(f"[{participant_id}] decision for {transaction_id}: {parts[0]}")
            affected_products = list(reservations.get(transaction_id, {}).keys())
            print_state("before", affected_products, stock, reservations)

            ack = f"ACK {transaction_id} {participant_id}"

            if parts[0] == "COMMIT":
                for product_id, quantity in reservations[transaction_id].items():
                    stock[product_id] -= quantity
                del reservations[transaction_id]
            else:
                if transaction_id in reservations:
                    del reservations[transaction_id]
            print_state("after ", affected_products, stock, reservations)

            participant.send_multipart([identity, ack.encode()])
            print(f"[{participant_id}] -> coordinator: {ack}")
