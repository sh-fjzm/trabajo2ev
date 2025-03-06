import csv
import time
import os
import threading
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor
from math import ceil

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

def load_numbers_from_csv(file_path):
    """Load numbers from the CSV file."""
    numbers = []
    try:
        print(f"Attempting to load numbers from {file_path}...")
        with open(file_path, 'r') as file:
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
        print(f"Error: File {file_path} not found.")
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

def split_workload(numbers, num_workers):
    """Split the numbers into equal chunks for each worker."""
    chunk_size = ceil(len(numbers) / num_workers)
    return [numbers[i:i + chunk_size] for i in range(0, len(numbers), chunk_size)]

def count_primes_in_chunk(chunk):
    """Count prime numbers in a chunk of numbers."""
    count = 0
    for num in chunk:
        if is_prime(num):
            count += 1
    return count

def count_primes_with_threads(numbers, num_threads):
    """Count prime numbers using multiple threads."""
    chunks = split_workload(numbers, num_threads)
    results = [0] * num_threads
    
    def worker(idx, chunk):
        results[idx] = count_primes_in_chunk(chunk)
    
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=worker, args=(i, chunks[i]))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    return sum(results)

def count_primes_with_threadpool(numbers, num_threads):
    """Count prime numbers using ThreadPoolExecutor."""
    chunks = split_workload(numbers, num_threads)
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = list(executor.map(count_primes_in_chunk, chunks))
    
    return sum(results)

def benchmark_threads(numbers, max_threads=16):
    """Benchmark different numbers of threads."""
    thread_counts = list(range(1, max_threads + 1))
    times = []
    
    for num_threads in thread_counts:
        print(f"Testing with {num_threads} threads...")
        start_time = time.time()
        prime_count = count_primes_with_threadpool(numbers, num_threads)
        end_time = time.time()
        
        processing_time = end_time - start_time
        times.append(processing_time)
        
        print(f"  Found {prime_count} primes in {processing_time:.4f} seconds")
    
    return thread_counts, times

def plot_results(thread_counts, times):
    """Plot the results of the benchmark."""
    plt.figure(figsize=(10, 6))
    plt.plot(thread_counts, times, marker='o')
    plt.title('Prime Number Counting Performance with Threads')
    plt.xlabel('Number of Threads')
    plt.ylabel('Processing Time (seconds)')
    plt.grid(True)
    plt.savefig('thread_performance.png')
    print(f"Plot saved as 'thread_performance.png'")
    plt.close()

def main():
    # Path to CSV file
    csv_path = "/home/isard/Descargas/numeros_aleatorios.csv"
    
    # Load numbers from CSV
    numbers = load_numbers_from_csv(csv_path)
    if not numbers:
        print("No numbers loaded. Exiting.")
        return
    
    print(f"Loaded {len(numbers)} numbers from CSV.")
    
    # Benchmark different numbers of threads
    thread_counts, times = benchmark_threads(numbers)
    
    # Plot the results
    plot_results(thread_counts, times)
    
    # Find the optimal number of threads
    optimal_idx = times.index(min(times))
    optimal_threads = thread_counts[optimal_idx]
    optimal_time = times[optimal_idx]
    
    print("\n--- Benchmark Results (Threads) ---")
    print(f"Optimal number of threads: {optimal_threads}")
    print(f"Best processing time: {optimal_time:.4f} seconds")

if __name__ == "__main__":
    main()