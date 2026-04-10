import socket
import time
import uuid
import os

HOST = '127.0.0.1'
PORT = 5000

##Unique consumer ID (persistent)
CONSUMER_ID_FILE = "consumer_id.txt"

##Load or create consumer ID
if os.path.exists(CONSUMER_ID_FILE):
    with open(CONSUMER_ID_FILE, "r") as f:
        consumer_id = f.read().strip()
else:
    consumer_id = str(uuid.uuid4())
    with open(CONSUMER_ID_FILE, "w") as f:
        f.write(consumer_id)

#Offset file
OFFSET_DIR = "offsets"
OFFSET_FILE = os.path.join(OFFSET_DIR, f"offset_{consumer_id}.txt")

os.makedirs(OFFSET_DIR, exist_ok=True)

# Load offset
if os.path.exists(OFFSET_FILE):
    with open(OFFSET_FILE, "r") as f:
        offset = int(f.read().strip())
else:
    offset = 0


def save_offset(offset):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(offset))


def start_consumer():
    global offset

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))

    print(f"Consumer ID: {consumer_id}")
    print(f"Starting from offset: {offset}\n")

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

                offset += 1
                save_offset(offset)  ##Persist offset

            time.sleep(2)

        except Exception as e:
            print(f"[ERROR] {e}")
            break

    client.close()


if __name__ == "__main__":
    start_consumer()