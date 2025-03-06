import csv
import time
import os
import multiprocessing as mp
import matplotlib.pyplot as plt
from concurrent.futures import ProcessPoolExecutor
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

def count_primes_with_processpool(numbers, num_processes):
    """Count prime numbers using ProcessPoolExecutor."""
    chunks = split_workload(numbers, num_processes)
    
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        results = list(executor.map(count_primes_in_chunk, chunks))
    
    return sum(results)

def benchmark_processes(numbers):
    """Benchmark from 1 to 12 processes."""
    process_counts = list(range(1, 13))  # 1 to 12 processes
    times = []
    prime_counts = []
    
    for num_processes in process_counts:
        print(f"Testing with {num_processes} processes...")
        start_time = time.time()
        prime_count = count_primes_with_processpool(numbers, num_processes)
        end_time = time.time()
        
        processing_time = end_time - start_time
        times.append(processing_time)
        prime_counts.append(prime_count)
        
        print(f"  Found {prime_count} primes in {processing_time:.4f} seconds")
    
    return process_counts, times, prime_counts

def plot_results(process_counts, times):
    """Plot the results of the benchmark."""
    plt.figure(figsize=(12, 7))
    plt.plot(process_counts, times, marker='o', linewidth=2, markersize=8)
    plt.title('Prime Number Counting Performance vs Number of Processes', fontsize=14)
    plt.xlabel('Number of Processes', fontsize=12)
    plt.ylabel('Processing Time (seconds)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # Add value labels on each point
    for i, (count, time) in enumerate(zip(process_counts, times)):
        plt.annotate(f'{time:.2f}s', 
                    (count, time),
                    textcoords="offset points",
                    xytext=(0,10),
                    ha='center')
    
    plt.savefig('process_performance.png', dpi=300, bbox_inches='tight')
    print(f"Plot saved as 'process_performance.png'")
    plt.close()

def main():
    # Path to CSV file
    csv_path = "numeros_aleatorios.csv"  # Modified to look in current directory first
    
    # Load numbers from CSV
    numbers = load_numbers_from_csv(csv_path)
    if not numbers:
        print("No numbers loaded. Generating random numbers for testing...")
        # Generate random numbers if no CSV is found
        numbers = [i for i in range(2, 100000)]
    
    print(f"Processing {len(numbers)} numbers...")
    
    # Benchmark different numbers of processes
    process_counts, times, prime_counts = benchmark_processes(numbers)
    
    # Plot the results
    plot_results(process_counts, times)
    
    # Find the optimal number of processes
    optimal_idx = times.index(min(times))
    optimal_processes = process_counts[optimal_idx]
    optimal_time = times[optimal_idx]
    
    print("\n--- Benchmark Results ---")
    print(f"Optimal number of processes: {optimal_processes}")
    print(f"Best processing time: {optimal_time:.4f} seconds")
    print(f"Number of primes found: {prime_counts[optimal_idx]}")
    
    # Print detailed results
    print("\nDetailed results:")
    print("Processes | Time (s) | Speedup")
    print("-" * 35)
    base_time = times[0]  # Single process time as baseline
    for proc, time in zip(process_counts, times):
        speedup = base_time / time
        print(f"{proc:^9d} | {time:^8.3f} | {speedup:^7.2f}x")

if __name__ == "__main__":
    main()