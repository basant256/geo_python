
import socket
import selectors

sel = selectors.DefaultSelector()


def accept(sock):
    """Accept a new client connection and register it for reading."""
    conn, addr = sock.accept()
    print(f"Accepted connection from {addr}")
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)


def read(conn):
    """Read data from a client socket, process the command, and send a response."""
    try:
        data = conn.recv(1024)
        if data:
            response = handle_command(data)
            conn.sendall(response)
        else:
            print("Closing connection")
            sel.unregister(conn)
            conn.close()
    except ConnectionResetError:
        sel.unregister(conn)
        conn.close()


def handle_command(data):
    """
    Minimal RESP parser for handling PING command.
    Returns a RESP simple string or bulk string as required by Redis protocol.
    """
    # RESP arrays start with '*'
    if data.startswith(b"*"):
        parts = data.split(b"\r\n")
        if len(parts) >= 4:
            cmd = parts[2].upper()
            if cmd == b"PING":
                # If there's an argument, echo it back as bulk string
                if len(parts) > 4 and parts[4]:
                    msg = parts[4]
                    return b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
                else:
                    # Simple string response for PING
                    return b"+PONG\r\n"
    # Default error for unsupported input
    return b"-ERR unknown command\r\n"


def main():
    host, port = "localhost", 6379
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((host, port))
    server_sock.listen()
    server_sock.setblocking(False)
    sel.register(server_sock, selectors.EVENT_READ, accept)

    print(f"Server listening on {host}:{port}")

    while True:
        events = sel.select(timeout=None)
        for key, mask in events:
            callback = key.data
            callback(key.fileobj)


if __name__ == "__main__":
    main()
