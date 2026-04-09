import socket

HOST = '127.0.0.1'
PORT = 5000


def start_producer():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))

    print("Connected to Queue Server")
    print("Type messages (type 'exit' to quit)\n")

    while True:
        message = input("Enter message: ")

        if message.lower() == 'exit':
            break

        client.send(message.encode('utf-8'))

    client.close()
    print("Disconnected from server")


if __name__ == "__main__":
    start_producer()