import socket
import threading
import time
import csv
import os
from math import ceil

# Server configuration
HOST = '0.0.0.0'  # Listen on all interfaces
PORT = 5000
BUFFER_SIZE = 1024
NUM_CLIENTS = 3

# Path to CSV file
CSV_PATH = "/home/isard/Descargas/numeros_aleatorios.csv"

def load_numbers_from_csv():
    """Load numbers from the CSV file."""
    numbers = []
    try:
        print(f"Attempting to load numbers from {CSV_PATH}...")
        with open(CSV_PATH, 'r') as file:
            csv_reader = csv.reader(file)
            row_count = 0
            for row in csv_reader:
                row_count += 1
                for item in row:
                    try:
                        numbers.append(int(item))
                    except ValueError:
                        # Skip non-integer values
                        pass
        print(f"Read {row_count} rows from CSV file.")
        return numbers
    except FileNotFoundError:
        print(f"Error: File {CSV_PATH} not found.")
        # Try to find the file in the current directory or other common locations
        alternative_paths = [
            "numeros_aleatorios.csv",
            os.path.join(os.path.expanduser("~"), "documents", "numeros_aleatorios.csv"),
            os.path.join(os.path.expanduser("~"), "Documents", "numeros_aleatorios.csv"),
            os.path.join(os.path.expanduser("~"), "Descargas", "numeros_aleatorios.csv"),
            os.path.join(os.path.expanduser("~"), "Downloads", "numeros_aleatorios.csv")
        ]
        
        for path in alternative_paths:
            if os.path.exists(path):
                print(f"Found alternative file at {path}")
                with open(path, 'r') as file:
                    csv_reader = csv.reader(file)
                    for row in csv_reader:
                        for item in row:
                            try:
                                numbers.append(int(item))
                            except ValueError:
                                pass
                print(f"Loaded {len(numbers)} numbers from alternative path.")
                return numbers
        
        return []
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []

def split_workload(numbers, num_clients):
    """Split the numbers into equal chunks for each client."""
    chunk_size = ceil(len(numbers) / num_clients)
    chunks = [numbers[i:i + chunk_size] for i in range(0, len(numbers), chunk_size)]
    
    # Print information about the chunks
    for i, chunk in enumerate(chunks):
        print(f"Chunk {i+1}: {len(chunk)} numbers")
    
    return chunks

def send_data(socket, data):
    """Send data with a size header to ensure complete transmission."""
    # Convert data to string and encode
    data_encoded = data.encode()
    # Send the size of the data first
    size = len(data_encoded)
    size_header = f"{size}".encode()
    socket.send(size_header)
    
    # Wait for acknowledgment
    socket.recv(BUFFER_SIZE)
    
    # Send the actual data
    socket.sendall(data_encoded)

def handle_client(client_socket, client_address, chunk):
    """Handle communication with a client."""
    print(f"Client connected from: {client_address[0]}")
    print(f"Sending {len(chunk)} numbers to client at {client_address[0]}")
    
    # Convert chunk to string format
    chunk_str = ','.join(map(str, chunk))
    
    # Send the chunk to the client
    send_data(client_socket, chunk_str)
    
    # Receive results from client
    response = client_socket.recv(BUFFER_SIZE).decode()
    primes_count, processing_time = map(float, response.split(','))
    
    client_socket.close()
    return int(primes_count), processing_time

def client_handler(idx, client_socket, client_address, chunk, results):
    """Thread function to handle a client and store the result."""
    results[idx] = handle_client(client_socket, client_address, chunk)

def main():
    # Load numbers from CSV
    numbers = load_numbers_from_csv()
    if not numbers:
        print("No numbers loaded. Exiting.")
        return
    
    print(f"Loaded {len(numbers)} numbers from CSV.")
    
    # Split workload
    chunks = split_workload(numbers, NUM_CLIENTS)
    
    # Create server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen(NUM_CLIENTS)
    
    print(f"Server started on {HOST}:{PORT}")
    print(f"Waiting for {NUM_CLIENTS} clients to connect...")
    
    # Wait for all clients to connect
    client_sockets = []
    client_addresses = []
    
    for _ in range(NUM_CLIENTS):
        client_socket, client_address = server_socket.accept()
        client_sockets.append(client_socket)
        client_addresses.append(client_address)
    
    print(f"All {NUM_CLIENTS} clients connected. Starting distributed processing.")
    
    # Start threads to handle each client
    threads = []
    results = [None] * NUM_CLIENTS
    
    for i in range(NUM_CLIENTS):
        thread = threading.Thread(
            target=client_handler,
            args=(i, client_sockets[i], client_addresses[i], chunks[i], results)
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Calculate total results
    total_primes = sum(result[0] for result in results)
    total_time = sum(result[1] for result in results)
    
    print("\n--- Results ---")
    print(f"Total prime numbers found: {total_primes}")
    print(f"Total processing time: {total_time:.4f} seconds")
    
    # Print individual client results
    for i, (primes, time_taken) in enumerate(results):
        print(f"Client {i+1} ({client_addresses[i][0]}): {primes} primes in {time_taken:.4f} seconds")
    
    server_socket.close()

if __name__ == "__main__":
    main()