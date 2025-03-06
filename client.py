import socket
import time
import sys

# Client configuration
SERVER_HOST = '127.0.0.1'  # Default to localhost, can be changed to server IP
PORT = 5000
BUFFER_SIZE = 1024  # Buffer size for receiving data chunks

def is_prime(n):
    """Check if a number is prime."""
    if n <= 1:
        return False
    if n <= 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True

def count_primes(numbers):
    """Count prime numbers in the given list."""
    count = 0
    for num in numbers:
        if is_prime(num):
            count += 1
    return count

def receive_data(socket):
    """Receive data with size header to ensure complete transmission."""
    # First receive the size of the data
    size_data = socket.recv(BUFFER_SIZE).decode()
    if not size_data:
        return ""
    
    # Send acknowledgment
    socket.send(b"ACK")
    
    # Parse the size
    size = int(size_data)
    
    # Receive the actual data in chunks
    chunks = []
    bytes_received = 0
    
    while bytes_received < size:
        chunk = socket.recv(min(BUFFER_SIZE, size - bytes_received))
        if not chunk:
            break
        chunks.append(chunk)
        bytes_received += len(chunk)
    
    # Combine all chunks and decode
    return b''.join(chunks).decode()

def main():
    # Allow specifying server IP as command line argument
    if len(sys.argv) > 1:
        global SERVER_HOST
        SERVER_HOST = sys.argv[1]
    
    # Create client socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        # Connect to server
        print(f"Connecting to server at {SERVER_HOST}:{PORT}...")
        client_socket.connect((SERVER_HOST, PORT))
        print("Connected to server.")
        
        # Receive chunk of numbers from server
        data = receive_data(client_socket)
        numbers = [int(num) for num in data.split(',') if num]
        
        print(f"Received {len(numbers)} numbers to process.")
        
        # Process the numbers
        start_time = time.time()
        prime_count = count_primes(numbers)
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        print(f"Found {prime_count} prime numbers.")
        print(f"Processing time: {processing_time:.4f} seconds")
        
        # Send results back to server
        result = f"{prime_count},{processing_time}"
        client_socket.send(result.encode())
        
        print("Results sent to server.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client_socket.close()

if __name__ == "__main__":
    main()