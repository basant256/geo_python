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


def parse_resp_array(data):
    """
    Parses a RESP array from bytes and returns a list of strings.
    Example input: b'*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n'
    Output: [b'ECHO', b'hey']
    """
    # Split by CRLF
    parts = data.split(b"\r\n")
    if not parts or not parts[0].startswith(b"*"):
        return None  # Not a RESP array

    items = []
    idx = 1  # Start after the array header
    while idx < len(parts):
        if parts[idx].startswith(b"$"):
            # Bulk string: next line is the value
            length = int(parts[idx][1:])
            value = parts[idx + 1]
            items.append(value)
            idx += 2
        else:
            idx += 1
    return items


def handle_command(data):
    """
    Parses RESP input and handles PING and ECHO commands.
    Returns a RESP-compliant response.
    """
    # Parse the RESP array into a list of arguments
    args = parse_resp_array(data)
    if not args or len(args) == 0:
        return b"-ERR unknown command\r\n"

    # Command name is always the first argument, case-insensitive
    cmd = args[0].upper()

    if cmd == b"PING":
        # If there's an argument, echo it back as bulk string
        if len(args) > 1:
            msg = args[1]
            return b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
        else:
            return b"+PONG\r\n"
    elif cmd == b"ECHO":
        # ECHO always returns its argument as a bulk string
        if len(args) == 2:
            msg = args[1]
            return b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
        else:
            return b"-ERR wrong number of arguments for 'echo' command\r\n"
    else:
        return b"-ERR unknown command\r\n"


def main():
    # Create a TCP socket
    host, port = "localhost", 6379
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((host, port))
    server_sock.listen()
    server_sock.setblocking(False)
    # Register the server socket for read events (new connections)
    sel.register(server_sock, selectors.EVENT_READ, accept)

    print(f"Server listening on {host}:{port}")

    # Event loop: handle events as they arrive
    while True:
        events = sel.select(timeout=None)
        for key, mask in events:
            callback = key.data
            callback(key.fileobj)


if __name__ == "__main__":
    main()
