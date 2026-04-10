import socket
import time
import uuid

HOST = '127.0.0.1'
PORT = 5000

##Unique consumer ID
consumer_id = str(uuid.uuid4())

##Offset tracking
offset = 0


def start_consumer():
    global offset

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))

    print(f"Consumer ID: {consumer_id}")
    print("Fetching messages...\n")

    while True:
        try:
            request = f"GET {consumer_id} {offset}"
            client.send(request.encode('utf-8'))

            data = client.recv(1024)
            message = data.decode('utf-8')

            if message == "EMPTY":
                print("No new messages...")
            else:
                msg_offset, msg = message.split("|")

                print(f"[RECEIVED] {msg} (offset {msg_offset})")

                offset += 1  ##Move forward

            time.sleep(2)

        except Exception as e:
            print(f"[ERROR] {e}")
            break

    client.close()


if __name__ == "__main__":
    start_consumer()