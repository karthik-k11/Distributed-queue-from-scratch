import socket
import threading
import os
import json

HOST = '0.0.0.0'
PORT = 5000
NUM_PARTITIONS = 3

partitions = [[] for _ in range(NUM_PARTITIONS)]
lock = threading.Lock()

groups = {}


##File Helpers

def get_partition_file(i):
    return f"partition_{i}.txt"


def get_offset_file(group_id):
    return f"offsets_{group_id}.txt"


def load_partitions():
    for i in range(NUM_PARTITIONS):
        file = get_partition_file(i)
        if os.path.exists(file):
            with open(file, "r") as f:
                partitions[i] = [line.strip() for line in f.readlines()]


def persist_message(partition_id, message):
    with open(get_partition_file(partition_id), "a") as f:
        f.write(message + "\n")


##Offset persistence

def load_offsets(group_id):
    file = get_offset_file(group_id)

    if os.path.exists(file):
        with open(file, "r") as f:
            return json.load(f)

    return {str(i): 0 for i in range(NUM_PARTITIONS)}


def save_offsets(group_id, offsets):
    file = get_offset_file(group_id)

    with open(file, "w") as f:
        json.dump(offsets, f)


##Partition assignment

def assign_partitions(consumers):
    assignments = {c: [] for c in consumers}

    for i in range(NUM_PARTITIONS):
        consumer = consumers[i % len(consumers)]
        assignments[consumer].append(i)

    return assignments


def rebalance_group(group_id):
    group = groups[group_id]

    consumers = group["consumers"]

    if not consumers:
        group["assignments"] = {}
        return

    group["assignments"] = assign_partitions(consumers)

    print(f"[REBALANCED] {group_id} → {group['assignments']}")


##Client Handler

def handle_client(client_socket, address):
    print(f"[NEW CONNECTION] {address}")

    while True:
        try:
            data = client_socket.recv(1024)
            if not data:
                break

            msg = data.decode('utf-8').strip()

            ##JOIN groups
            if msg.startswith("JOIN"):
                parts = msg.split()

                if len(parts) != 3:
                    client_socket.send(b"INVALID_JOIN")
                    continue

                _, group_id, consumer_id = parts

                with lock:
                    if group_id not in groups:
                        groups[group_id] = {
                            "consumers": [],
                            "assignments": {},
                            "offsets": load_offsets(group_id)
                        }

                    if consumer_id not in groups[group_id]["consumers"]:
                        groups[group_id]["consumers"].append(consumer_id)

                        rebalance_group(group_id)

                    print(f"[GROUP {group_id}] {groups[group_id]}")

                client_socket.send(b"JOINED")

            ##GET
            elif msg.startswith("GET"):
                parts = msg.split()

                if len(parts) != 3:
                    client_socket.send(b"INVALID_GET")
                    continue

                _, group_id, consumer_id = parts

                with lock:
                    group = groups.get(group_id)

                    if not group:
                        client_socket.send(b"NO_GROUP")
                        continue

                    partitions_assigned = group["assignments"].get(consumer_id, [])

                    sent = False

                    for p in partitions_assigned:
                        offset = group["offsets"][str(p)]

                        if offset < len(partitions[p]):
                            message = partitions[p][offset]

                            group["offsets"][str(p)] += 1
                            save_offsets(group_id, group["offsets"])

                            response = f"{p}|{offset}|{message}"
                            client_socket.send(response.encode())
                            sent = True
                            break

                    if not sent:
                        client_socket.send(b"EMPTY")

            ##PRODUCER
            else:
                with lock:
                    partition_id = sum(len(p) for p in partitions) % NUM_PARTITIONS

                    partitions[partition_id].append(msg)
                    persist_message(partition_id, msg)

                    print(f"[APPENDED] {msg} → P{partition_id}")

        except Exception as e:
            print(f"[ERROR] {e}")
            break

    client_socket.close()


def start_server():
    load_partitions()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)

    print(f"[STARTED] {HOST}:{PORT}")

    while True:
        client_socket, addr = server.accept()

        threading.Thread(
            target=handle_client,
            args=(client_socket, addr)
        ).start()


if __name__ == "__main__":
    start_server()