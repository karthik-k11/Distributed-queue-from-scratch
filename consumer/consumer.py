import socket
import time

HOST = '127.0.0.1'
PORT = 5000

##Choose partition manually (0, 1, 2)
partition_id = int(input("Enter partition (0/1/2): "))

offset = 0


def start_consumer():
    global offset

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))

    print(f"Reading from Partition {partition_id}\n")

    while True:
        try:
            request = f"GET {partition_id} {offset}"
            client.send(request.encode('utf-8'))

            data = client.recv(1024)
            message = data.decode('utf-8')

            if message == "EMPTY":
                print("No messages...")
            else:
                msg_offset, msg = message.split("|")
                print(f"[RECEIVED] {msg} (offset {msg_offset})")

                offset += 1

            time.sleep(2)

        except Exception as e:
            print(f"[ERROR] {e}")
            break

    client.close()


if __name__ == "__main__":
    start_consumer()