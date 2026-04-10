import socket
import threading

HOST = '0.0.0.0'
PORT = 5000

message_queue = []

##Create a lock
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

            ##Consumer request
            if message == "GET":

                queue_lock.acquire()  ##LOCK START

                if message_queue:
                    msg = message_queue.pop(0)
                    client_socket.send(msg.encode('utf-8'))
                    print(f"[SENT TO CONSUMER] {msg}")
                else:
                    client_socket.send(b"EMPTY")

                queue_lock.release()  ##LOCK END

            else:
                ##Producer request

                queue_lock.acquire()  ##LOCK START

                message_queue.append(message)
                print(f"[QUEUE SIZE] {len(message_queue)}")
                print(f"[STORED] {message}")

                queue_lock.release()  ##LOCK END

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