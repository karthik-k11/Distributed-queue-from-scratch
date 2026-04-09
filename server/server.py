import socket
import threading

HOST = '0.0.0.0'   
PORT = 5000       


def handle_client(client_socket, address):
    print(f"[NEW CONNECTION] {address} connected.")

    while True:
        try:
            ##Receive data from client
            data = client_socket.recv(1024)

            if not data:
                print(f"[DISCONNECTED] {address}")
                break

            message = data.decode('utf-8')
            print(f"[RECEIVED] From {address}: {message}")

        except Exception as e:
            print(f"[ERROR] {e}")
            break

    client_socket.close()


def start_server():
    ##Create TCP socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    ##Bind to host and port
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