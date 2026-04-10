import socket
import threading
import os

HOST = '0.0.0.0'
PORT = 5000

NUM_PARTITIONS = 3

##Partition logs
partitions = [[] for _ in range(NUM_PARTITIONS)]

##Track next partition (round-robin)
current_partition = 0

lock = threading.Lock()


##File names
def get_partition_file(i):
    return f"partition_{i}.txt"


##Load partitions from disk
def load_partitions():
    for i in range(NUM_PARTITIONS):
        file = get_partition_file(i)

        if os.path.exists(file):
            with open(file, "r") as f:
                partitions[i] = [line.strip() for line in f.readlines()]

    print("[LOADED PARTITIONS]")


##Save message
def persist_message(partition_id, message):
    with open(get_partition_file(partition_id), "a") as f:
        f.write(message + "\n")


def handle_client(client_socket, address):
    global current_partition

    print(f"[NEW CONNECTION] {address} connected.")

    while True:
        try:
            data = client_socket.recv(1024)

            if not data:
                print(f"[DISCONNECTED] {address}")
                break

            message = data.decode('utf-8')

            ##Consumer request: GET partition offset
            if message.startswith("GET"):
                _, partition_id, offset = message.split()
                partition_id = int(partition_id)
                offset = int(offset)

                lock.acquire()

                if offset < len(partitions[partition_id]):
                    msg = partitions[partition_id][offset]
                    response = f"{offset}|{msg}"
                    client_socket.send(response.encode('utf-8'))

                    print(f"[SEND] P{partition_id} → {msg}")
                else:
                    client_socket.send(b"EMPTY")

                lock.release()

            else:
                ##Producer message (round-robin)
                lock.acquire()

                partition_id = current_partition
                partitions[partition_id].append(message)
                persist_message(partition_id, message)

                print(f"[APPENDED] {message} → Partition {partition_id}")

                current_partition = (current_partition + 1) % NUM_PARTITIONS

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

    print(f"[STARTED] Server on {HOST}:{PORT}")

    while True:
        client_socket, address = server.accept()

        thread = threading.Thread(
            target=handle_client,
            args=(client_socket, address)
        )

        thread.start()


if __name__ == "__main__":
    start_server()