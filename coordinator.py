import sys
import zmq



# Static product catalog: packetId -> list of (participantId, productId, units per packet unit).
# The coordinator knows which participant owns each product, but not the current stock.
CATALOG = {
    # Single products
    "FLIGHT_A": [("P0", "FLIGHT_A", 1)],
    "FLIGHT_B": [("P0", "FLIGHT_B", 1)],
    "HOTEL_A":  [("P1", "HOTEL_A", 1)],
    "HOTEL_B":  [("P1", "HOTEL_B", 1)],
    "TOUR_A":   [("P2", "TOUR_A", 1)],
    "TOUR_B":   [("P2", "TOUR_B", 1)],
    # Packages
    "TRIP_A":    [("P0", "FLIGHT_A", 1), ("P1", "HOTEL_A", 1)],
    "TRIP_B":    [("P0", "FLIGHT_B", 1), ("P1", "HOTEL_B", 1)],
    "TRIP_FULL": [("P0", "FLIGHT_A", 1), ("P1", "HOTEL_A", 1), ("P2", "TOUR_A", 1)],
}

def get_participants_dictionary(args):
    participants_dict = {}
    for arg in args:
        parts = arg.split(":")
        if len(parts) != 2:
            sys.exit(f"[COORD] error: '{arg}' must have the form <participant_id>:<port>")
        participant_id, participant_port = parts
        
        if participant_id in participants_dict:
            sys.exit(f"[COORD] error: duplicate participant '{participant_id}'")
        participants_dict[participant_id] = participant_port
        
    return participants_dict


def send_prepare(dealers, pending, transaction_id):
    for participant in pending[transaction_id]["participants"]:
        participant_products = ""

        for product_name, demand in pending[transaction_id]["demands"][participant].items():
            participant_products += f"{product_name} {demand} "
        dealers[participant].send_string(f"PREPARE {transaction_id} {participant_products}")
        print(f"[COORD] -> participant {participant}: PREPARE {transaction_id} {participant_products}")


def send_decision(dealers, pending, decision, transaction_id):
    for pid in pending[transaction_id]["participants"]:
        dealers[pid].send_string(f"{decision} {transaction_id}")
        print(f"[COORD] -> {pid}: {decision} {transaction_id}")


def get_decision(pending, transaction_id):
    if pending[transaction_id]["prepared"] == pending[transaction_id]["votes counted"]:
        return "COMMIT"
    else:
        return "ABORT"


def check_products(products):
    product_ids = products[0::2]
    for product_id in product_ids:
        if product_id not in CATALOG:
            return False
    return True


def aggregate_demands(products):
    product_ids = products[0::2]
    quantities = products[1::2]
    demands = {}

    for i in range(len(product_ids)):
        product_id = product_ids[i]
        quantity = quantities[i]
        for product in CATALOG[product_id]:
            participant = product[0]
            product_name = product[1]
            units = product[2]
            if participant not in demands:
                demands[participant] = {}

            if product_name in demands[participant]:
                demands[participant][product_name] += int(units) * int(quantity)
            else:
                demands[participant][product_name] = int(units) * int(quantity)
    return demands

if len(sys.argv) != 5:
    sys.exit("usage: python coordinator.py <client_port> <participant_id>:<port> ...")

client_port = sys.argv[1]
participants_dict = get_participants_dictionary(sys.argv[2:])
num_participants = len(participants_dict.keys())  

context = zmq.Context()

router = context.socket(zmq.ROUTER)
router.bind(f"tcp://*:{client_port}")

poller = zmq.Poller()
poller.register(router, zmq.POLLIN)

dealers = {}
for participant_id, participant_port in participants_dict.items():
    dealer = context.socket(zmq.DEALER)
    dealer.connect(f"tcp://localhost:{participant_port}")
    poller.register(dealer, zmq.POLLIN)
    dealers[participant_id] = dealer

print(f"[COORD] listening for clients on port {client_port}, participants on ports {list(participants_dict.values())}")

pending = {}

while True: 
    events = dict(poller.poll(500))
    if router in events:
        identity, message = router.recv_multipart()
        message = message.decode()
        parts = message.split()
        if parts[0] == "TRANSACTION":
            transaction_id = parts[1]
            products = parts[2::]
            print(f"[COORD] <- client {identity.decode()}: {message}")
            # check products
            if not check_products(products):
                reply = f"ABORT {transaction_id}"
                router.send_multipart([identity, reply.encode()])
                print(f"[COORD] unknown product in {transaction_id}, -> client: {reply}")
            else:
                demands = aggregate_demands(products)
                print(f"[COORD] aggregated demands for {transaction_id}:")
                for pid, pid_demands in demands.items():
                    print(f"[COORD]   {pid}: {pid_demands}")

                pending[transaction_id] = {
                    "client id": identity,
                    "demands": demands,
                    "participants": list(demands.keys()),
                    "decision": None,
                    "votes counted": 0,
                    "prepared": 0,
                    "ACK": 0
                }
                send_prepare(dealers, pending, transaction_id)
    for dealer_id, dealer in dealers.items():
        if dealer in events:
            parts = dealer.recv_string().split()
            vote = parts[0]
            transaction_id = parts[1]
            participant_id = parts[2]
            print(f"[COORD] <- {participant_id}: {vote} {transaction_id}")

            if vote not in ["PREPARED", "ABORT", "ACK"]:
                continue    
            elif vote in ["PREPARED", "ABORT"]:
                pending[transaction_id]["votes counted"] += 1
                if vote == "PREPARED":
                    pending[transaction_id]["prepared"] += 1
                selected = len(pending[transaction_id]["participants"])
                print(f"[COORD] {transaction_id}: votes {pending[transaction_id]['votes counted']}/{selected} "
                      f"({pending[transaction_id]['prepared']} PREPARED)")
                if pending[transaction_id]["votes counted"] == selected and pending[transaction_id]["decision"] is None:
                    decision = get_decision(pending, transaction_id)
                    pending[transaction_id]["decision"] = decision
                    print(f"[COORD] decision for {transaction_id}: {decision} "
                          f"({pending[transaction_id]['prepared']}/{selected} PREPARED)")
                    send_decision(dealers, pending, decision, transaction_id)
                
            
            elif vote == "ACK":
                pending[transaction_id]["ACK"] += 1
                selected = len(pending[transaction_id]["participants"])
                print(f"[COORD] {transaction_id}: ACKs {pending[transaction_id]['ACK']}/{selected}")
                if pending[transaction_id]["ACK"] == selected:
                    client_reply = f"{pending[transaction_id]['decision']} {transaction_id}"
                    router.send_multipart([pending[transaction_id]["client id"], client_reply.encode()])
                    print(f"[COORD] -> client: {client_reply}")
                    # Transaction is over so delete it from pending.
                    del pending[transaction_id]