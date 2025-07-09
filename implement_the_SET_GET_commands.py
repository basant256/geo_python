
import socket
import selectors

sel = selectors.DefaultSelector()

# This dictionary will store our key-value pairs in memory
database = {}


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
    Parses a RESP array from bytes and returns a list of byte strings.
    Example input: b'*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n'
    Output: [b'ECHO', b'hey']
    """
    # Split the data by CRLF (\r\n)
    parts = data.split(b"\r\n")
    # Remove any empty strings from the end (caused by trailing \r\n)
    parts = [p for p in parts if p != b""]
    # If the first part doesn't start with *, it's not a valid RESP array
    if not parts or not parts[0].startswith(b"*"):
        return None

    items = []
    idx = 1  # Start after the array header
    while idx < len(parts):
        if parts[idx].startswith(b"$"):
            # The next line is the value
            length = int(parts[idx][1:])
            value = parts[idx + 1]
            items.append(value)
            idx += 2
        else:
            idx += 1
    return items


def handle_command(data):
    """
    Parses RESP input and handles PING, ECHO, SET, and GET commands.
    Returns a RESP-compliant response.
    """
    # Parse the RESP array into a list of arguments
    args = parse_resp_array(data)
    if not args or len(args) == 0:
        return b"-ERR unknown command\r\n"

    # The first argument is the command name (like PING, ECHO, SET, GET)
    cmd = args[0].upper()

    if cmd == b"PING":
        # If there's an argument, echo it back as bulk string
        if len(args) > 1:
            msg = args[1]
            return b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
        else:
            return b"+PONG\r\n"
    elif cmd == b"ECHO":
        # ECHO returns its argument as a bulk string
        if len(args) == 2:
            msg = args[1]
            return b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
        else:
            return b"-ERR wrong number of arguments for 'echo' command\r\n"
    elif cmd == b"SET":
        # SET stores a value for a key
        # It needs exactly 3 arguments: SET key value
        if len(args) == 3:
            key = args[1]
            value = args[2]
            # Store the value in the database dictionary
            database[key] = value
            # Respond with a simple string OK
            return b"+OK\r\n"
        else:
            return b"-ERR wrong number of arguments for 'set' command\r\n"
    elif cmd == b"GET":
        # GET retrieves the value for a key
        # It needs exactly 2 arguments: GET key
        if len(args) == 2:
            key = args[1]
            # If the key exists, return its value as a bulk string
            if key in database:
                value = database[key]
                return b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
            else:
                # If the key does not exist, return a null bulk string
                return b"$-1\r\n"
        else:
            return b"-ERR wrong number of arguments for 'get' command\r\n"
    else:
        # If the command is not recognized, return an error
        return b"-ERR unknown command\r\n"


def main():
    # Create a TCP socket for the server
    host, port = "localhost", 6379
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Allow the address to be reused
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Bind the socket to the host and port
    server_sock.bind((host, port))
    # Start listening for connections
    server_sock.listen()
    # Set the socket to non-blocking mode
    server_sock.setblocking(False)
    # Register the server socket for read events (new connections)
    sel.register(server_sock, selectors.EVENT_READ, accept)

    print(f"Server listening on {host}:{port}")

    # This is the event loop: it waits for events and handles them
    while True:
        events = sel.select(timeout=None)
        for key, mask in events:
            callback = key.data
            callback(key.fileobj)


if __name__ == "__main__":
    main()
