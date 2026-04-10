import socket
import threading
import os

HOST = '0.0.0.0'
PORT = 5000

LOG_FILE = "message_log.txt"

message_log = []

lock = threading.Lock()


##Load messages from file at startup
def load_messages():
    global message_log

    if not os.path.exists(LOG_FILE):
        return

    with open(LOG_FILE, "r") as f:
        message_log = [line.strip() for line in f.readlines()]

    print(f"[LOADED] {len(message_log)} messages from disk")


##Append message to file
def persist_message(message):
    with open(LOG_FILE, "a") as f:
        f.write(message + "\n")


def handle_client(client_socket, address):
    print(f"[NEW CONNECTION] {address} connected.")

    while True:
        try:
            data = client_socket.recv(1024)

            if not data:
                print(f"[DISCONNECTED] {address}")
                break

            message = data.decode('utf-8')

            ##Consumer request
            if message.startswith("GET"):
                _, consumer_id, offset = message.split()
                offset = int(offset)

                lock.acquire()

                if offset < len(message_log):
                    msg = message_log[offset]
                    response = f"{offset}|{msg}"
                    client_socket.send(response.encode('utf-8'))

                    print(f"[SEND] {msg} (offset {offset})")
                else:
                    client_socket.send(b"EMPTY")

                lock.release()

            else:
                ##Producer message
                lock.acquire()

                message_log.append(message)
                persist_message(message)

                print(f"[APPENDED] {message} (offset {len(message_log)-1})")

                lock.release()

        except Exception as e:
            print(f"[ERROR] {e}")
            break

    client_socket.close()


def start_server():
    load_messages()  ##Load from disk

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)

    print(f"[STARTED] Server listening on {HOST}:{PORT}")

    while True:
        client_socket, address = server.accept()

        client_thread = threading.Thread(
            target=handle_client,
            args=(client_socket, address)
        )

        client_thread.start()

        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")


if __name__ == "__main__":
    start_server()