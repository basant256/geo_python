import socket
import selectors
import time
import sys  # For reading command-line arguments

sel = selectors.DefaultSelector()

# This dictionary will store our key-value pairs in memory
database = {}
# This dictionary will store expiry times for keys (in milliseconds since epoch)
expiry = {}

# Default values for dir and dbfilename
config = {
    b"dir": b".",           # Default directory is current directory
    b"dbfilename": b"dump.rdb"  # Default filename
}

def parse_args():
    """
    Parses command-line arguments to set config['dir'] and config['dbfilename'].
    Example: --dir /tmp/redis-files --dbfilename dump.rdb
    """
    args = sys.argv[1:]  # Skip the script name
    i = 0
    while i < len(args):
        if args[i] == "--dir" and i + 1 < len(args):
            config[b"dir"] = args[i + 1].encode()
            i += 2
        elif args[i] == "--dbfilename" and i + 1 < len(args):
            config[b"dbfilename"] = args[i + 1].encode()
            i += 2
        else:
            i += 1

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
    parts = data.split(b'\r\n')
    parts = [p for p in parts if p != b'']
    if not parts or not parts[0].startswith(b'*'):
        return None
    items = []
    idx = 1
    while idx < len(parts):
        if parts[idx].startswith(b'$'):
            length = int(parts[idx][1:])
            value = parts[idx + 1]
            items.append(value)
            idx += 2
        else:
            idx += 1
    return items

def resp_bulk_string(value):
    """Helper to encode a value as a RESP bulk string."""
    return b'$' + str(len(value)).encode() + b'\r\n' + value + b'\r\n'

def resp_array(items):
    """Helper to encode a list of RESP bulk strings as a RESP array."""
    resp = b'*' + str(len(items)).encode() + b'\r\n'
    for item in items:
        resp += resp_bulk_string(item)
    return resp

def handle_command(data):
    """
    Parses RESP input and handles PING, ECHO, SET, GET, and CONFIG GET commands.
    Also supports PX expiry for SET.
    Returns a RESP-compliant response.
    """
    args = parse_resp_array(data)
    if not args or len(args) == 0:
        return b'-ERR unknown command\r\n'

    cmd = args[0].upper()

    if cmd == b'PING':
        if len(args) > 1:
            msg = args[1]
            return resp_bulk_string(msg)
        else:
            return b'+PONG\r\n'
    elif cmd == b'ECHO':
        if len(args) == 2:
            msg = args[1]
            return resp_bulk_string(msg)
        else:
            return b'-ERR wrong number of arguments for \'echo\' command\r\n'
    elif cmd == b'SET':
        if len(args) >= 3:
            key = args[1]
            value = args[2]
            database[key] = value
            if key in expiry:
                del expiry[key]
            if len(args) >= 5 and args[3].upper() == b'PX':
                try:
                    px_ms = int(args[4])
                    expiry[key] = int(time.time() * 1000) + px_ms
                except ValueError:
                    return b'-ERR PX value is not an integer\r\n'
            return b'+OK\r\n'
        else:
            return b'-ERR wrong number of arguments for \'set\' command\r\n'
    elif cmd == b'GET':
        if len(args) == 2:
            key = args[1]
            if key in expiry:
                now = int(time.time() * 1000)
                if now >= expiry[key]:
                    if key in database:
                        del database[key]
                    del expiry[key]
                    return b'$-1\r\n'
            if key in database:
                value = database[key]
                return resp_bulk_string(value)
            else:
                return b'$-1\r\n'
        else:
            return b'-ERR wrong number of arguments for \'get\' command\r\n'
    elif cmd == b'CONFIG':
        # Handle CONFIG GET <parameter>
        if len(args) == 3 and args[1].upper() == b'GET':
            param = args[2].lower()
            # Only support "dir" and "dbfilename"
            if param in config:
                # Return RESP array: [param, value]
                return resp_array([param, config[param]])
            else:
                # If unknown parameter, return empty array
                return b'*0\r\n'
        else:
            return b'-ERR wrong number of arguments for \'config get\' command\r\n'
    else:
        return b'-ERR unknown command\r\n'

def main():
    # Parse command-line arguments for dir and dbfilename
    parse_args()
    host, port = 'localhost', 6379
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
