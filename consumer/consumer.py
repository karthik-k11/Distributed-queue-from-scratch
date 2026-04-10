import socket
import threading
import uuid

HOST = '0.0.0.0'
PORT = 5000

message_queue = []
in_flight = {}

queue_lock = threading.Lock()


def handle_client(client_socket, address):
    print(f"[NEW CONNECTION] {address} connected.")

    while True:
        try:
            data = client_socket.recv(1024)

            if not data:
                print(f"[DISCONNECTED] {address}")
                break

            message = data.decode('utf-8')

            ##Consumer GET request
            if message == "GET":

                queue_lock.acquire()

                if message_queue:
                    msg = message_queue.pop(0)
                    msg_id = str(uuid.uuid4())

                    in_flight[msg_id] = msg

                    response = f"{msg_id}|{msg}"
                    client_socket.send(response.encode('utf-8'))

                    print(f"[SENT] {msg} (ID: {msg_id})")

                else:
                    client_socket.send(b"EMPTY")

                queue_lock.release()

            ##ACK from consumer
            elif message.startswith("ACK"):

                _, msg_id = message.split("|")

                queue_lock.acquire()

                if msg_id in in_flight:
                    print(f"[ACK RECEIVED] {in_flight[msg_id]}")
                    del in_flight[msg_id]

                queue_lock.release()

            else:
                ##Producer message
                queue_lock.acquire()

                message_queue.append(message)
                print(f"[STORED] {message}")

                queue_lock.release()

        except Exception as e:
            print(f"[ERROR] {e}")
            break

    client_socket.close()


def start_server():
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