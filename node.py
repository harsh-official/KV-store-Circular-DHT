import sys
import socket
import threading
import hashlib
import json
import time
import sqlite3  # Add this import
import os

# Utility: Hash function
def hash_function(key, m=10):
    """Hashes the key to an m-bit identifier."""
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** m)

# Chord Node Class
class Node:
    def __init__(self, ip, port, m=10, replication_factor=3):
        self.ip = ip
        self.port = port
        self.node_id = hash_function(f"{ip}:{port}")
        self.m = m
        self.replication_factor = replication_factor  # Changed back to configurable replication
        self.successor = (ip, port)
        self.predecessor = None
        self.finger_table = [(ip, port)] * m
        self.finger_starts = [(self.node_id + 2**i) % (2**self.m) for i in range(m)]
        self.lock = threading.Lock()
        self.db_conn = sqlite3.connect('node_data.db', check_same_thread=False)
        self.create_tables()
        self.successor_list = []
        self.successor_list_size = replication_factor
        self.stabilize_interval = 1
        self.last_finger_fix = time.time()  # Add this line
        self.thread_pool = []  # Add thread pool
        self.max_threads = 50  # Maximum number of threads
        self.cleanup_interval = 60  # Cleanup every 60 seconds
        self.stabilize_backoff = 1  # Add backoff timer
        self.last_stabilize = time.time()
        self.predecessor_check_interval = 2  # Check predecessor every 2 seconds

    def create_tables(self):
        """Create tables in the SQLite database."""
        with self.db_conn:
            self.db_conn.execute('DROP TABLE IF EXISTS data_store')
            self.db_conn.execute('''CREATE TABLE data_store (
                                    key TEXT PRIMARY KEY,
                                    value TEXT,
                                    timestamp REAL)''')  # Add timestamp column

    def store_key_in_db(self, key, value):
        """Store a key-value pair with timestamp"""
        with self.lock:
            timestamp = time.time()
            with self.db_conn:
                self.db_conn.execute('''REPLACE INTO data_store 
                    (key, value, timestamp) VALUES (?, ?, ?)''', 
                    (key, value, timestamp))

    def retrieve_key_from_db(self, key):
        """Retrieve a value from the database by key."""
        cursor = self.db_conn.cursor()
        cursor.execute('SELECT value FROM data_store WHERE key = ?', (key,))
        row = cursor.fetchone()
        return row[0] if row else None

    def delete_key_from_db(self, key):
        """Delete a key-value pair from the database."""
        with self.db_conn:
            self.db_conn.execute('DELETE FROM data_store WHERE key = ?', (key,))  # Use raw key

    # Node Operations
    def start_server(self):
        """Start the server with thread management."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.ip, self.port))
        server.listen(5)
        print(f"Node started on {self.ip}:{self.port} (ID: {self.node_id})")

        # Start thread cleanup
        threading.Thread(target=self.cleanup_threads, daemon=True).start()

        while True:
            try:
                conn, addr = server.accept()
                self.cleanup_dead_threads()
                
                if len(self.thread_pool) < self.max_threads:
                    thread = threading.Thread(target=self.handle_request, args=(conn,))
                    thread.daemon = True  # Make thread daemon
                    self.thread_pool.append(thread)
                    thread.start()
                else:
                    print("Thread pool full, dropping connection")
                    conn.close()
            except Exception as e:
                print(f"Error in server: {e}")
                time.sleep(1)

    def cleanup_threads(self):
        """Periodically cleanup dead threads"""
        while True:
            self.cleanup_dead_threads()
            time.sleep(self.cleanup_interval)

    def cleanup_dead_threads(self):
        """Remove dead threads from the pool"""
        self.thread_pool = [t for t in self.thread_pool if t.is_alive()]

    def handle_request(self, conn):
        try:
            conn.settimeout(5)
            data = conn.recv(4096).decode()
            if not data:
                return
                
            request = json.loads(data)
            response = {"status": "error", "message": "Invalid command"}

            if request["command"] == "store_key":
                key = request["key"]
                value = request["value"]
                is_replica = request.get("is_replica", False)
                
                # Find the primary node responsible for the key
                successor = self.find_successor(hash_function(key))
                
                if successor == (self.ip, self.port):
                    # Store locally
                    self.store_key_in_db(key, value)
                    
                    # Only replicate if this is not already a replica operation
                    if not is_replica:
                        replicas = self.get_successors(self.replication_factor - 1)
                        for replica in replicas:
                            self.remote_store_key(replica, key, value, is_replica=True)
                    
                    response = {"status": "success", "message": "Key stored successfully"}
                else:
                    # Forward to responsible node
                    response = self.remote_store_key(successor, key, value, is_replica=is_replica)

            elif request["command"] == "retrieve_key":
                try:
                    key = request["key"]
                    key_id = hash_function(key)
                    if self.is_responsible_for_key(key_id):
                        value = self.retrieve_key_from_db(key)
                        response = {"status": "success", "value": value}
                    else:
                        successor = self.find_successor(key_id)
                        response = self.remote_retrieve_key(successor, key)
                except Exception as e:
                    response = {"status": "error", "message": str(e)}

            elif request["command"] == "delete_key":
                try:
                    key = request["key"]
                    key_id = hash_function(key)
                    if self.is_responsible_for_key(key_id):
                        self.delete_key_from_db(key)
                        self.replicate_delete_key(key)
                        response = {"status": "success", "message": "Key deleted successfully"}
                    else:
                        successor = self.find_successor(key_id)
                        response = self.remote_delete_key(successor, key)
                except Exception as e:
                    response = {"status": "error", "message": str(e)}

            elif request["command"] == "find_successor":
                id_ = request["id"]
                response = {"successor": self.find_successor(id_)}

            elif request["command"] == "update_predecessor":
                self.predecessor = request["predecessor"]
                self.replicate_all_keys_to_predecessors()

            elif request["command"] == "update_successor":
                self.successor = request["successor"]
                self.replicate_all_keys()

            elif request["command"] == "get_predecessor":
                response = {"predecessor": self.predecessor}

            elif request["command"] == "ping":
                response = {"status": "alive"}

            elif request["command"] == "notify":
                # Handle notification from another node claiming to be our predecessor
                possible_predecessor = tuple(request["predecessor"])
                if self.predecessor is None or self.is_between_exclusive(
                    hash_function(f"{possible_predecessor[0]}:{possible_predecessor[1]}"),
                    hash_function(f"{self.predecessor[0]}:{self.predecessor[1]}"),
                    self.node_id
                ):
                    old_predecessor = self.predecessor
                    self.predecessor = possible_predecessor
                    print(f"Updated predecessor to: {self.predecessor}")
                response = {"status": "notified"}

            elif request["command"] == "get_successor_list":
                # Ensure successor list contains tuples
                sanitized_list = [tuple(s) if isinstance(s, list) else s for s in self.successor_list]
                response = {"successor_list": sanitized_list}

            conn.send(json.dumps(response).encode())
        except socket.timeout:
            print("Request handling timed out")
        except Exception as e:
            print(f"Error handling request: {e}")
            try:
                conn.send(json.dumps({"status": "error", "message": str(e)}).encode())
            except:
                pass
        finally:
            conn.close()
            # Remove self from thread pool if present
            if threading.current_thread() in self.thread_pool:
                self.thread_pool.remove(threading.current_thread())

    def is_responsible_for_key(self, key_id):
        """Check if this node is responsible for the given key ID."""
        if self.predecessor is None:
            return True
        
        pred_id = hash_function(f"{self.predecessor[0]}:{self.predecessor[1]}")
        succ_id = hash_function(f"{self.successor[0]}:{self.successor[1]}")
        
        # If we're the only node or responsible for the key
        if self.predecessor == (self.ip, self.port) or self.successor == (self.ip, self.port):
            return True
            
        # Normal case
        if pred_id < self.node_id:
            return pred_id < key_id <= self.node_id
        # Wrap around case
        return key_id > pred_id or key_id <= self.node_id

    def find_successor(self, id_):
        """Find the successor of a given ID."""
        if self.node_id < id_ <= hash_function(f"{self.successor[0]}:{self.successor[1]}"):
            return self.successor
        else:
            closest_node = self.closest_preceding_node(id_)
            if closest_node == (self.ip, self.port):
                return self.successor
            return self.remote_find_successor(closest_node, id_)

    def closest_preceding_node(self, id_):
        """Find the closest preceding node for a given ID using the finger table."""
        for i in range(self.m - 1, -1, -1):
            finger = self.finger_table[i]
            finger_id = hash_function(f"{finger[0]}:{finger[1]}")
            if self.is_between_exclusive(finger_id, self.node_id, id_):
                return finger
        return (self.ip, self.port)

    def remote_find_successor(self, node, id_):
        """Query another node for the successor of a given ID."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(node)
                s.send(json.dumps({"command": "find_successor", "id": id_}).encode())
                response = json.loads(s.recv(4096).decode())
                return tuple(response["successor"])
        except Exception as e:
            print(f"Error contacting node {node}: {e}")
            return self.successor

    def remote_store_key(self, node, key, value, retries=3, is_replica=False):
        """Store a key-value pair at a remote node with retries."""
        for attempt in range(retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    s.connect(node)
                    request = {
                        "command": "store_key",
                        "key": key,
                        "value": value,
                        "is_replica": is_replica
                    }
                    s.send(json.dumps(request).encode())
                    response = json.loads(s.recv(4096).decode())
                    return response
            except socket.timeout:
                print(f"Timeout while contacting node {node}, attempt {attempt + 1}")
            except Exception as e:
                print(f"Error storing key at node {node}, attempt {attempt + 1}: {e}")
        return {"status": "error", "message": "Request timed out after multiple attempts"}

    def remote_retrieve_key(self, node, key, retries=3):
        """Retrieve a key-value pair from a remote node with retries."""
        for attempt in range(retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(node)
                    request = {
                        "command": "retrieve_key",
                        "key": key
                    }
                    s.send(json.dumps(request).encode())
                    response = json.loads(s.recv(4096).decode())
                    return response
            except socket.timeout:
                print(f"Timeout while contacting node {node}, attempt {attempt + 1}")
            except Exception as e:
                print(f"Error retrieving key from node {node}, attempt {attempt + 1}: {e}")
        return {"status": "error", "message": "Request timed out after multiple attempts"}

    def remote_delete_key(self, node, key, retries=3):
        """Delete a key-value pair from a remote node with retries."""
        for attempt in range(retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(node)
                    s.send(json.dumps({"command": "delete_key", "key": key}).encode())  # Use raw key
                    response = json.loads(s.recv(4096).decode())
                    return response
            except socket.timeout:
                print(f"Timeout while contacting node {node}, attempt {attempt + 1}")
            except Exception as e:
                print(f"Error deleting key at node {node}, attempt {attempt + 1}: {e}")
        return {"status": "error", "message": "Request timed out after multiple attempts"}

    def replicate_key(self, key, value):
        """Replicate key to next r-1 successors"""
        replicas = self.get_successors(self.replication_factor - 1)
        for replica in replicas:
            self.remote_store_key(replica, key, value, is_replica=True)

    def replicate_delete_key(self, key):
        """Simplified delete replication - only delete from successor"""
        if self.successor != (self.ip, self.port):
            self.remote_delete_key(self.successor, key)

    def get_successors(self, count):
        """Get the next 'count' successors from the finger table."""
        successors = []
        for i in range(self.m):
            if len(successors) >= count:
                break
            if self.finger_table[i] not in successors and self.finger_table[i] != (self.ip, self.port):
                successors.append(self.finger_table[i])
        return successors

    def get_predecessors(self, count):
        """Get the previous 'count' predecessors from the finger table."""
        predecessors = []
        current_node = self.predecessor
        while current_node and len(predecessors) < count:
            if current_node not in predecessors and current_node != (self.ip, self.port):
                predecessors.append(current_node)
            current_node = self.remote_get_predecessor(current_node)
        return predecessors

    # Chord Network Maintenance
    def join(self, known_node=None):
        """Join the Chord network via a known node."""
        if known_node:
            # Find successor through known node
            self.successor = self.remote_find_successor(known_node, self.node_id)
            if self.successor:
                # Get predecessor from successor
                self.predecessor = self.remote_get_predecessor(self.successor)
                # Get successor list from successor
                succ_list = self.remote_get_successor_list(self.successor)
                if succ_list:
                    self.successor_list = [self.successor] + succ_list[:self.successor_list_size-1]
                else:
                    self.successor_list = [self.successor]
                # Notify successor
                self.remote_notify(self.successor, (self.ip, self.port))
                # Initialize finger table
                self.init_finger_table(known_node)
                print(f"Joined network. Successor: {self.successor}, Predecessor: {self.predecessor}")
                self.replicate_all_keys()
        else:
            print("Started a new Chord network as standalone node.")
            # Set self as both successor and predecessor when starting new network
            self.successor = (self.ip, self.port)
            self.predecessor = (self.ip, self.port)
            self.init_finger_table()
            self.successor_list = []

    def check_successor(self):
        """Check if the successor is alive."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(self.successor)
                s.send(json.dumps({"command": "ping"}).encode())
                response = json.loads(s.recv(4096).decode())
                return response.get("status") == "alive"
        except Exception:
            return False

    def stabilize(self):
        """Enhanced stabilization with better ring maintenance"""
        while True:
            try:
                current_time = time.time()
                
                # Check predecessor periodically
                if current_time - self.last_stabilize > self.predecessor_check_interval:
                    if self.predecessor and not self.check_node_alive(self.predecessor):
                        print(f"Predecessor {self.predecessor} appears to be down")
                        self.predecessor = None
                    self.last_stabilize = current_time

                # Check successor and update successor list
                if not self.check_successor():
                    self.handle_successor_failure()
                else:
                    self.update_successor_list()
                    x = self.remote_get_predecessor(self.successor)
                    
                    if x:
                        x_id = hash_function(f"{x[0]}:{x[1]}")
                        if self.is_between_exclusive(x_id, self.node_id, 
                            hash_function(f"{self.successor[0]}:{self.successor[1]}")):
                            self.successor = x
                            self.update_successor_list()

                # Notify successor
                if self.successor != (self.ip, self.port):
                    self.remote_notify(self.successor, (self.ip, self.port))
                
                # Reset backoff on successful stabilization
                self.stabilize_backoff = 1

            except Exception as e:
                print(f"Error in stabilization: {e}")
                time.sleep(self.stabilize_backoff)
                self.stabilize_backoff = min(self.stabilize_backoff * 2, 8)

            time.sleep(1)

    def handle_successor_failure(self):
        """Handle successor failure with proper list maintenance"""
        print(f"Successor {self.successor} appears to be down")
        new_successor = self.find_new_successor()
        
        if new_successor != (self.ip, self.port):
            old_successor = self.successor
            self.successor = new_successor
            print(f"Updated successor from {old_successor} to {new_successor}")
            
            # Get successor list from new successor
            new_list = self.remote_get_successor_list(new_successor)
            if new_list:
                self.successor_list = [new_successor] + new_list[:self.successor_list_size-1]
            else:
                self.successor_list = [new_successor]
            
            # Repair replicas
            self.repair_replicas()
        else:
            print("Becoming standalone node")
            # Set self as both successor and predecessor in standalone mode
            self.successor = (self.ip, self.port)
            self.predecessor = (self.ip, self.port)
            self.successor_list = []

    def transfer_keys_after_failure(self):
        """Transfer keys after a successor failure"""
        try:
            with self.lock:
                cursor = self.db_conn.cursor()
                cursor.execute('SELECT key, value FROM data_store')
                for key, value in cursor.fetchall():
                    # Only transfer keys that should belong to the new successor
                    key_id = hash_function(key)
                    if not self.is_responsible_for_key(key_id):
                        self.remote_store_key(self.successor, key, value)
        except Exception as e:
            print(f"Error transferring keys after failure: {e}")

    def check_node_alive(self, node, retries=2):
        """Check if a node is alive with retries"""
        for _ in range(retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(2)  # Short timeout for ping
                    s.connect(node)
                    s.send(json.dumps({"command": "ping"}).encode())
                    response = json.loads(s.recv(1024).decode())
                    return response.get("status") == "alive"
            except Exception:
                time.sleep(0.5)  # Brief pause between retries
        return False

    def remote_get_predecessor(self, node):
        """Get the predecessor of another node."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(node)
                s.send(json.dumps({"command": "get_predecessor"}).encode())
                response = json.loads(s.recv(4096).decode())
                return tuple(response["predecessor"]) if response["predecessor"] else None
        except Exception as e:
            print(f"Error contacting node {node}: {e}")
            return None

    def remote_notify(self, node, possible_predecessor, retries=2):
        """Notify another node with retries"""
        last_error = None
        for _ in range(retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(2)
                    s.connect(node)
                    s.send(json.dumps({
                        "command": "notify",
                        "predecessor": possible_predecessor
                    }).encode())
                    response = json.loads(s.recv(1024).decode())
                    return response.get("status") == "notified"
            except Exception as e:
                last_error = e
                time.sleep(0.5)
        
        # Only print error after all retries fail
        print(f"Error notifying node {node} after {retries} attempts: {last_error}")
        return False

    def replicate_all_keys(self):
        """Simplified replication of all keys to successor"""
        with self.lock:
            cursor = self.db_conn.cursor()
            cursor.execute('SELECT key, value FROM data_store')
            for key, value in cursor.fetchall():
                self.replicate_key(key, value)

    def replicate_all_keys_to_predecessors(self):
        pass

    def replicate_key_to_predecessors(self, key, value):
        pass

    def repair_replicas(self):
        """Repair replicas after node failures"""
        try:
            with self.lock:
                cursor = self.db_conn.cursor()
                cursor.execute('SELECT key, value FROM data_store')
                for key, value in cursor.fetchall():
                    # Check if we should still store this key
                    key_id = hash_function(key)
                    responsible_node = self.find_successor(key_id)
                    
                    if responsible_node == (self.ip, self.port):
                        # We're primary, ensure replicas exist
                        self.replicate_key(key, value)
                    elif not self.is_replica_for(key_id):
                        # We shouldn't have this key
                        self.delete_key_from_db(key)

        except Exception as e:
            print(f"Error repairing replicas: {e}")

    def is_replica_for(self, key_id):
        """Check if this node should be a replica for the given key"""
        primary = self.find_successor(key_id)
        replicas = self.get_successors_of_node(primary, self.replication_factor - 1)
        return (self.ip, self.port) in replicas

    def get_successors_of_node(self, node, count):
        """Get the next count successors of a given node"""
        successors = []
        current = node
        
        for _ in range(count):
            next_node = self.remote_find_successor(current, 
                (hash_function(f"{current[0]}:{current[1]}") + 1) % (2**self.m))
            if next_node and next_node not in successors:
                successors.append(next_node)
                current = next_node
            else:
                break
                
        return successors

    def menu(self):
        """Display a menu for interacting with the node."""
        while True:
            try:
                print("\nMenu:")
                print("1. Add/Update Key")
                print("2. Retrieve Key")
                print("3. Delete Key")
                print("4. Print Finger Table")
                print("5. Print Node Info")
                print("6. Exit")
                
                choice = input("Enter your choice: ")

                if choice == "1":
                    key = input("Enter key: ")
                    value = input("Enter value: ")
                    try:
                        key_id = hash_function(key)
                        successor = self.find_successor(key_id)
                        if successor == (self.ip, self.port):
                            self.store_key_in_db(key, value)
                            self.replicate_key(key, value)
                            print("Key stored successfully")
                        else:
                            response = self.remote_store_key(successor, key, value)
                            if response.get("status") == "success":
                                print("Key stored successfully")
                            else:
                                print(f"Error storing key: {response.get('message', 'Unknown error')}")
                    except Exception as e:
                        print(f"Error storing key: {e}")

                elif choice == "2":
                    key = input("Enter key: ")
                    try:
                        key_id = hash_function(key)
                        if self.is_responsible_for_key(key_id):
                            value = self.retrieve_key_from_db(key)
                            print(f"Value: {value}")
                        else:
                            successor = self.find_successor(key_id)
                            response = self.remote_retrieve_key(successor, key)
                            print(f"Retrieved value: {response.get('value')}")
                    except Exception as e:
                        print(f"Error retrieving key: {e}")

                elif choice == "3":
                    key = input("Enter key: ")
                    key_id = hash_function(key)
                    successor = self.find_successor(key_id)
                    response = self.remote_delete_key(successor, key)
                    print(f"Response: {response}")

                elif choice == "4":
                    self.print_finger_table()

                elif choice == "5":
                    print(f"\nNode ID: {self.node_id}")
                    print(f"IP:Port: {self.ip}:{self.port}")
                    print(f"Successor: {self.successor}")
                    print(f"Predecessor: {self.predecessor}")
                    print(f"Successor List: {self.successor_list}")

                elif choice == "6":
                    print("Exiting...")
                    os._exit(0)

                else:
                    print("Invalid choice. Please try again.")

            except Exception as e:
                print(f"Error in menu operation: {e}")
                continue

    def send_request(self, request):
        """Send a request to the node itself."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.ip, self.port))
                s.send(json.dumps(request).encode())
                response = json.loads(s.recv(4096).decode())
                return response
        except Exception as e:
            print(f"Error sending request: {e}")
            return None

    def init_finger_table(self, known_node=None):
        """Initialize the finger table entries."""
        if known_node:
            # First finger is the successor
            self.finger_table[0] = self.remote_find_successor(known_node, (self.node_id + 2**0) % (2**self.m))
            
            # Initialize remaining fingers
            for i in range(1, self.m):
                start = (self.node_id + 2**(i)) % (2**self.m)
                
                # If start is between us and finger[i-1], then finger[i] = finger[i-1]
                if self.is_between(start, self.node_id, 
                    hash_function(f"{self.finger_table[i-1][0]}:{self.finger_table[i-1][1]}")):
                    self.finger_table[i] = self.finger_table[i-1]
                else:
                    self.finger_table[i] = self.remote_find_successor(known_node, start)
        else:
            # If this is the first node, all fingers point to self
            self.finger_table = [(self.ip, self.port)] * self.m

    def fix_fingers(self):
        """Enhanced periodic finger table maintenance"""
        i = 0
        while True:
            try:
                current_time = time.time()
                if current_time - self.last_finger_fix > 30:  # Full table refresh every 30 seconds
                    print("Performing full finger table refresh")
                    self.init_finger_table(self.successor)
                    self.last_finger_fix = current_time
                else:
                    # Regular incremental updates
                    start = (self.node_id + 2**i) % (2**self.m)
                    new_finger = self.find_successor(start)
                    if new_finger and self.check_node_alive(new_finger):
                        self.finger_table[i] = new_finger
                    i = (i + 1) % self.m

            except Exception as e:
                print(f"Error fixing finger {i}: {e}")
            time.sleep(0.1)

    def is_between(self, id_, start, end):
        """Check if id_ is in [start, end] on the circle."""
        if start <= end:
            return start <= id_ <= end
        return id_ >= start or id_ <= end

    def is_between_exclusive(self, id_, start, end):
        """Check if id_ is in (start, end) on the circle."""
        if start < end:
            return start < id_ < end
        return id_ > start or id_ < end

    def print_finger_table(self):
        """Debug method to print finger table entries"""
        print("\nFinger Table:")
        for i in range(self.m):
            start = self.finger_starts[i]
            successor = self.finger_table[i]
            print(f"i={i}: start={start}, successor={successor}")

    def update_successor_list(self):
        """Maintain a consistent successor list"""
        try:
            if self.successor == (self.ip, self.port):
                self.successor_list = []
                return

            # Start with immediate successor
            new_list = [self.successor]
            current = self.successor

            # Try to get next successors
            for _ in range(self.successor_list_size - 1):
                succ_list = self.remote_get_successor_list(current)
                if succ_list:
                    # Convert list elements to tuples
                    succ_list = [tuple(s) if isinstance(s, list) else s for s in succ_list]
                    if succ_list[0] != (self.ip, self.port):
                        next_succ = succ_list[0]
                        if next_succ not in new_list:
                            new_list.append(next_succ)
                            current = next_succ
                        else:
                            break
                else:
                    break

            self.successor_list = new_list

        except Exception as e:
            print(f"Error updating successor list: {e}")
            self.successor_list = [self.successor]  # Fallback to just immediate successor

    def find_new_successor(self):
        """Enhanced successor finding with better validation"""
        # First try successor list
        if self.successor_list:
            for succ in self.successor_list:
                if succ != self.successor and self.check_node_alive(succ):
                    return succ

        # Then try finger table entries
        alive_fingers = set()  # Use set to avoid duplicates
        for i in range(self.m):
            finger = self.finger_table[i]
            if finger != self.successor and finger != (self.ip, self.port):
                if self.check_node_alive(finger):
                    alive_fingers.add(finger)

        if alive_fingers:
            # Find the closest alive finger
            closest = min(alive_fingers, key=lambda f: 
                abs(hash_function(f"{f[0]}:{f[1]}") - self.node_id))
            return closest

        # If all else fails, become standalone
        print("Warning: No live nodes found, becoming standalone node")
        self.predecessor = None
        self.successor_list = []
        return (self.ip, self.port)

    def remote_get_successor_list(self, node):
        """Get successor list from a remote node"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # Ensure node is a tuple
                if isinstance(node, list):
                    node = tuple(node)
                s.connect(node)
                s.send(json.dumps({"command": "get_successor_list"}).encode())
                response = json.loads(s.recv(4096).decode())
                successor_list = response.get("successor_list", [])
                # Convert any list elements to tuples
                return [tuple(s) if isinstance(s, list) else s for s in successor_list]
        except Exception as e:
            print(f"Error getting successor list from {node}: {e}")
            return []

# Main execution
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python node.py <ip_address> <port> [<known_ip> <known_port>]")
        sys.exit(1)

    ip = sys.argv[1]
    port = int(sys.argv[2])
    node = Node(ip, port)

    if len(sys.argv) == 5:
        known_ip = sys.argv[3]
        known_port = int(sys.argv[4])
        node.join((known_ip, known_port))
    else:
        node.join()

    threading.Thread(target=node.start_server).start()
    threading.Thread(target=node.stabilize).start()
    threading.Thread(target=node.fix_fingers).start()

    # Start the menu for user interaction
    node.menu()
