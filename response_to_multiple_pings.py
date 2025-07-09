import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept()
    # Uncomment this to pass the first stage
    while True:
        # wait for client
        request: bytes = connection.recv(512)
        data: str = request.decode()
        # print(f"Received data: {data!r}")
        if not data:
            break
        # Respond to PING command
        if "ping" in data.lower():
            connection.send("+PONG\r\n".encode())
        # connection.close()


if __name__ == "__main__":
    main()
