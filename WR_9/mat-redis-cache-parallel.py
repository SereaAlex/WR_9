import os
import time
import shutil
import redis
import concurrent.futures

# Initialize Redis connection
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Function to simulate processing an input file
def process_input_file(input_file):
    time.sleep(1)  # Simulate processing time
    return f"Processed {input_file}"

# Function to generate output file based on input file
def generate_output_file(input_file):
    output_file = input_file.replace(".in", ".out")
    with open(output_file, 'w') as f:
        f.write(f"Output for {input_file}")
    return output_file

# Function to process input file using Redis for caching
def process_input_with_redis(input_file):
    # Check if input_file is in Redis cache
    if redis_client.exists(input_file):
        return redis_client.get(input_file).decode('utf-8')

    # If not in cache, process the file
    processed_result = process_input_file(input_file)
    
    # Store processed result in Redis cache with expiration of 1 hour (3600 seconds)
    redis_client.setex(input_file, 3600, processed_result)
    
    return processed_result

# Function to process all input files in parallel
def process_input_files_parallel(input_files):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Submit each input file processing to the executor
        results = executor.map(process_input_with_redis, input_files)
    
    return list(results)


def main():
    # Example input directory
    input_dir = "C:/Users/a_ser/OneDrive/Desktop/WR_9"
    input_files = os.listdir(input_dir)
    
    # Process input files in parallel using Redis for caching
    start_time = time.time()
    processed_results = process_input_files_parallel(input_files)
    total_time = time.time() - start_time
    
    # Print results (just for demonstration)
    for result in processed_results:
        print(result)
    
    print(f"Total processing time: {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
