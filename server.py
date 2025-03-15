import socket
import csv
import time
import os
from typing import List, Tuple
import math

def split_file(filename: str, num_parts: int) -> List[List[int]]:
    numbers = []
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            numbers.extend(map(int, row))
    
    # Calculate split points
    chunk_size = math.ceil(len(numbers) / num_parts)
    return [numbers[i:i + chunk_size] for i in range(0, len(numbers), chunk_size)]

def main():
    # Server configuration
    HOST = '10.20.20.101'  # Server IP
    PORT = 65432
    NUM_CLIENTS = 3

    # Create server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow reuse of address
    server_socket.bind((HOST, PORT))
    server_socket.listen(NUM_CLIENTS)
    
    print(f"Server listening on {HOST}:{PORT}")

    # Try different possible locations for the CSV file
    possible_paths = [
        os.path.expanduser("~/Documentos/numeros_aleatorios.csv"),
        os.path.expanduser("~/Descargas/numeros_aleatorios.csv"),
        os.path.expanduser("~/Personal/numeros_aleatorios.csv")
    ]
    
    file_path = None
    for path in possible_paths:
        if os.path.exists(path):
            file_path = path
            break
    
    if not file_path:
        print("Error: CSV file not found in any of the expected locations")
        return

    try:
        # Split the data
        data_chunks = split_file(file_path, NUM_CLIENTS)
        
        # Wait for all clients to connect
        clients = []
        addresses = []
        
        print(f"Waiting for {NUM_CLIENTS} clients to connect...")
        
        for i in range(NUM_CLIENTS):
            client_socket, address = server_socket.accept()
            print(f"Client {i+1} connected from {address}")
            clients.append(client_socket)
            addresses.append(address)

        total_primes = 0
        total_time = 0

        # Send data to each client and receive results
        for i, (client, chunk) in enumerate(zip(clients, data_chunks)):
            try:
                # Send data with terminating character
                data_str = ','.join(map(str, chunk)) + '\n'
                client.sendall(data_str.encode())
                
                # Receive results
                result = client.recv(1024).decode().strip()
                if result:
                    primes, time_taken = map(float, result.split(','))
                    total_primes += int(primes)
                    total_time += time_taken
                    print(f"Client {i+1} found {int(primes)} primes in {time_taken:.2f} seconds")
            except Exception as e:
                print(f"Error with client {i+1}: {e}")
            finally:
                client.close()

        print(f"\nTotal Results:")
        print(f"Total prime numbers found: {total_primes}")
        print(f"Total processing time: {total_time:.2f} seconds")
        print(f"Average time per client: {total_time/NUM_CLIENTS:.2f} seconds")

    except Exception as e:
        print(f"Server error: {e}")
    finally:
        # Close server socket
        server_socket.close()

if __name__ == "__main__":
    main()