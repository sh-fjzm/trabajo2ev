import csv
import time
import os

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

def count_primes_sequential(numbers):
    """Count prime numbers in the given list using sequential processing."""
    count = 0
    for num in numbers:
        if is_prime(num):
            count += 1
    return count

def main():
    # Path to CSV file
    csv_path = "/home/isard/Descargas/numeros_aleatorios.csv"
    
    # Load numbers from CSV
    numbers = load_numbers_from_csv(csv_path)
    if not numbers:
        print("No numbers loaded. Exiting.")
        return
    
    print(f"Loaded {len(numbers)} numbers from CSV.")
    
    # Process the numbers sequentially
    start_time = time.time()
    prime_count = count_primes_sequential(numbers)
    end_time = time.time()
    
    processing_time = end_time - start_time
    
    print("\n--- Results (Sequential Processing) ---")
    print(f"Total prime numbers found: {prime_count}")
    print(f"Processing time: {processing_time:.4f} seconds")

if __name__ == "__main__":
    main()