import socket
import time
import multiprocessing as mp
import array
import math
from typing import List, Set

def is_prime(n: int) -> bool:
    """Optimized prime number check using wheel factorization"""
    if n < 2:
        return False
    if n == 2 or n == 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    
    # Use wheel factorization (6k ± 1)
    for i in range(5, int(math.sqrt(n)) + 1, 6):
        if n % i == 0 or n % (i + 2) == 0:
            return False
    return True

def process_chunk(start_idx: int, end_idx: int, numbers: array.array) -> int:
    """Process a chunk of numbers using optimized prime checking"""
    return sum(1 for n in numbers[start_idx:end_idx] if is_prime(n))

def parallel_prime_count(numbers: List[int], num_processes: int) -> int:
    """Count primes using parallel processing with improved chunking"""
    if not numbers:
        return 0
    
    # Convert to array for more efficient memory usage
    num_array = array.array('i', numbers)
    chunk_size = math.ceil(len(numbers) / num_processes)
    
    # Create chunks based on index ranges rather than splitting the array
    chunks = [
        (i * chunk_size, min((i + 1) * chunk_size, len(numbers)))
        for i in range(num_processes)
    ]
    
    # Use process pool for parallel processing
    with mp.Pool(processes=num_processes) as pool:
        results = pool.starmap(
            process_chunk,
            [(start, end, num_array) for start, end in chunks]
        )
    
    return sum(results)

def main():
    # Client configuration
    SERVER_HOST = '10.20.20.101'
    SERVER_PORT = 65432
    BUFFER_SIZE = 65536  # Increased buffer size for faster data transfer

    # Create client socket with optimized settings
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)
    
    try:
        # Connect to server
        print(f"Connecting to server at {SERVER_HOST}:{SERVER_PORT}")
        client_socket.connect((SERVER_HOST, SERVER_PORT))
        print("Connected to server")

        # Receive data with larger buffer
        data = []
        while True:
            chunk = client_socket.recv(BUFFER_SIZE).decode()
            if not chunk:
                break
            if '\n' in chunk:
                data.append(chunk[:chunk.index('\n')])
                break
            data.append(chunk)
        
        data_str = ''.join(data)
        if not data_str:
            raise ValueError("Received empty data from server")
            
        # Convert received data to numbers more efficiently
        numbers = [int(x) for x in data_str.split(',')]
        print(f"Received {len(numbers)} numbers to process")

        # Process numbers using optimized parallel processing
        start_time = time.time()
        num_processes = mp.cpu_count()  # Use all available CPU cores
        prime_count = parallel_prime_count(numbers, num_processes)
        processing_time = time.time() - start_time

        # Send results back to server
        result = f"{prime_count},{processing_time}"
        client_socket.sendall(result.encode())
        
        print(f"Found {prime_count} prime numbers in {processing_time:.2f} seconds")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client_socket.close()

if __name__ == "__main__":
    main()
