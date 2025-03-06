import csv
import time
import os
import multiprocessing as mp
import threading
import matplotlib.pyplot as plt
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from math import ceil
import numpy as np

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

def count_primes_in_chunk_numpy(chunk):
    """Count prime numbers in a chunk using NumPy for potential optimization."""
    arr = np.array(chunk)
    is_prime_arr = np.ones(len(arr), dtype=bool)
    is_prime_arr[arr <= 1] = False
    is_prime_arr[(arr % 2 == 0) & (arr != 2)] = False
    is_prime_arr[(arr % 3 == 0) & (arr != 3)] = False
    
    for i in range(len(arr)):
        if is_prime_arr[i]:
            n = arr[i]
            if n <= 3:
                continue
            limit = int(n**0.5) + 1
            j = 5
            while j <= limit:
                if n % j == 0 or n % (j + 2) == 0:
                    is_prime_arr[i] = False
                    break
                j += 6
    return np.sum(is_prime_arr)

# Mover la función `process_chunk_with_threads` fuera de `count_primes_hybrid`
def process_chunk_with_threads(chunk, num_threads):
    """Process a chunk of data using threads to count primes."""
    thread_chunks = split_workload(chunk, num_threads)
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = list(executor.map(count_primes_in_chunk, thread_chunks))
    return sum(results)

def count_primes_hybrid(numbers, num_processes, num_threads_per_process):
    """
    Hybrid approach using both processes and threads.
    First divides work among processes, then each process uses threads.
    """
    process_chunks = split_workload(numbers, num_processes)
    
    # Ejecutar procesos
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        results = list(executor.map(process_chunk_with_threads, process_chunks, [num_threads_per_process] * num_processes))
    
    return sum(results)

def benchmark_hybrid(numbers, max_processes=4, max_threads=4):
    """Benchmark different combinations of processes and threads."""
    results = []
    
    max_processes = min(max_processes, mp.cpu_count())
    
    for num_processes in range(1, max_processes + 1):
        for num_threads in range(1, max_threads + 1):
            total_workers = num_processes * num_threads
            print(f"Testing with {num_processes} processes × {num_threads} threads = {total_workers} workers...")
            
            start_time = time.time()
            prime_count = count_primes_hybrid(numbers, num_processes, num_threads)
            end_time = time.time()
            
            processing_time = end_time - start_time
            results.append((num_processes, num_threads, total_workers, processing_time, prime_count))
            
            print(f"  Found {prime_count} primes in {processing_time:.4f} seconds")
    
    return results

def plot_hybrid_results(results):
    """Plot the results of the hybrid benchmark."""
    processes = [r[0] for r in results]
    threads = [r[1] for r in results]
    total_workers = [r[2] for r in results]
    times = [r[3] for r in results]
    
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(111, projection='3d')
    surf = ax.plot_trisurf(processes, threads, times, cmap='viridis', edgecolor='none')
    
    ax.set_xlabel('Number of Processes')
    ax.set_ylabel('Number of Threads per Process')
    ax.set_zlabel('Processing Time (seconds)')
    ax.set_title('Hybrid Processing Performance')
    
    fig.colorbar(surf, ax=ax, shrink=0.5, aspect=5)
    plt.savefig('hybrid_performance_3d.png')
    print(f"3D plot saved as 'hybrid_performance_3d.png'")
    
    plt.figure(figsize=(10, 6))
    plt.scatter(total_workers, times, c=times, cmap='viridis')
    plt.xlabel('Total Number of Workers (Processes × Threads)')
    plt.ylabel('Processing Time (seconds)')
    plt.title('Hybrid Processing Performance by Total Workers')
    plt.colorbar(label='Processing Time (seconds)')
    plt.grid(True)
    plt.savefig('hybrid_performance_2d.png')
    print(f"2D plot saved as 'hybrid_performance_2d.png'")
    
    plt.close('all')

def main():
    # Path to CSV file
    csv_path = "/home/isard/Descargas/numeros_aleatorios.csv"
    
    # Load numbers from CSV
    numbers = load_numbers_from_csv(csv_path)
    if not numbers:
        print("No numbers loaded. Exiting.")
        return
    
    print(f"Loaded {len(numbers)} numbers from CSV.")
    
    # Benchmark hybrid approach
    results = benchmark_hybrid(numbers)
    
    # Plot the results
    plot_hybrid_results(results)
    
    # Find the optimal configuration
    optimal_result = min(results, key=lambda x: x[3])
    optimal_processes, optimal_threads, total_workers, optimal_time, prime_count = optimal_result
    
    print("\n--- Benchmark Results (Hybrid) ---")
    print(f"Optimal configuration: {optimal_processes} processes × {optimal_threads} threads = {total_workers} workers")
    print(f"Best processing time: {optimal_time:.4f} seconds")
    print(f"Total prime numbers found: {prime_count}")

if __name__ == "__main__":
    main()
