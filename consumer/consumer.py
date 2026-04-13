import socket
import time
import uuid

HOST = '127.0.0.1'
PORT = 5000

group_id = input("Enter group ID: ")
consumer_id = str(uuid.uuid4())


def start_consumer():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))

    ##JOIN group
    join_msg = f"JOIN {group_id} {consumer_id}"
    client.send(join_msg.encode())

    print(f"Joined group {group_id} as {consumer_id}\n")

    while True:
        try:
            get_msg = f"GET {group_id} {consumer_id}"
            client.send(get_msg.encode())

            data = client.recv(1024)
            msg = data.decode()

            if msg == "EMPTY":
                print("No messages...")
            else:
                p, offset, message = msg.split("|")
                print(f"[P{p}] {message} (offset {offset})")

            time.sleep(2)

        except Exception as e:
            print(e)
            break

    client.close()


if __name__ == "__main__":
    start_consumer()