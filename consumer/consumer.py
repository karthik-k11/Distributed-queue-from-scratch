import socket
import time

HOST = '127.0.0.1'
PORT = 5000


def start_consumer():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))

    print("Connected to Queue Server")
    print("Fetching messages...\n")

    while True:
        try:
            ##Ask server for message
            client.send(b"GET")

            data = client.recv(1024)
            message = data.decode('utf-8')

            if message == "EMPTY":
                print("No messages in queue...")
            else:
                print(f"[RECEIVED] {message}")

            time.sleep(2)

        except Exception as e:
            print(f"[ERROR] {e}")
            break

    client.close()


if __name__ == "__main__":
    start_consumer()