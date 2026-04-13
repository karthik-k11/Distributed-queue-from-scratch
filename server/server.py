import socket
import threading
import os

HOST = '0.0.0.0'
PORT = 5000
NUM_PARTITIONS = 3

# Partition storage
partitions = [[] for _ in range(NUM_PARTITIONS)]

lock = threading.Lock()

##Consumer groups
groups = {}


def get_partition_file(i):
    return f"partition_{i}.txt"


def load_partitions():
    for i in range(NUM_PARTITIONS):
        file = get_partition_file(i)
        if os.path.exists(file):
            with open(file, "r") as f:
                partitions[i] = [line.strip() for line in f.readlines()]


def persist_message(partition_id, message):
    with open(get_partition_file(partition_id), "a") as f:
        f.write(message + "\n")


##Partition assignment
def assign_partitions(consumers):
    assignments = {c: [] for c in consumers}

    for i in range(NUM_PARTITIONS):
        consumer = consumers[i % len(consumers)]
        assignments[consumer].append(i)

    return assignments


def handle_client(client_socket, address):
    print(f"[NEW CONNECTION] {address}")

    while True:
        try:
            data = client_socket.recv(1024)
            if not data:
                break

            msg = data.decode('utf-8')

            ##JOIN
            if msg.startswith("JOIN"):
                _, group_id, consumer_id = msg.split()

                lock.acquire()

                if group_id not in groups:
                    groups[group_id] = {
                        "consumers": [],
                        "assignments": {},
                        "offsets": {i: 0 for i in range(NUM_PARTITIONS)}
                    }

                if consumer_id not in groups[group_id]["consumers"]:
                    groups[group_id]["consumers"].append(consumer_id)

                # Assign partitions
                groups[group_id]["assignments"] = assign_partitions(
                    groups[group_id]["consumers"]
                )

                print(f"[GROUP {group_id}] {groups[group_id]}")

                lock.release()

                client_socket.send(b"JOINED")

            ##GET
            elif msg.startswith("GET"):
                _, group_id, consumer_id = msg.split()

                lock.acquire()

                group = groups.get(group_id)

                if not group:
                    client_socket.send(b"NO_GROUP")
                    lock.release()
                    continue

                partitions_assigned = group["assignments"].get(consumer_id, [])

                sent = False

                for p in partitions_assigned:
                    offset = group["offsets"][p]

                    if offset < len(partitions[p]):
                        message = partitions[p][offset]
                        group["offsets"][p] += 1

                        response = f"{p}|{offset}|{message}"
                        client_socket.send(response.encode())
                        sent = True
                        break

                if not sent:
                    client_socket.send(b"EMPTY")

                lock.release()

            ##PRODUCER
            else:
                lock.acquire()

                ##Round-robin insert
                partition_id = sum(len(p) for p in partitions) % NUM_PARTITIONS

                partitions[partition_id].append(msg)
                persist_message(partition_id, msg)

                print(f"[APPENDED] {msg} → P{partition_id}")

                lock.release()

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