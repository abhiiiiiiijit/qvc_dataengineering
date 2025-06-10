import time
from concurrent.futures import ThreadPoolExecutor
from rough2 import transform_dict_string_leaves

def benchmark_workers(dictionary, func, max_workers):
    start = time.time()
    transform_dict_string_leaves(dictionary.copy(), func, max_workers)
    return time.time() - start

# Test with different worker counts
workers_range = [1, 2, 4, 8, 16, 32, 64, 128]
large_dict = {
        'level1a': {
            'level2a': {f'level3a{i}': f'value{i}' for i in range(1000000)},
            'level2b': 'root_value'
        },
        'level1b': {f'level2c{i}': {f'level3b{j}': f'nested{j}' for j in range(50)} 
                    for i in range(200)}
    }
results = {}
for workers in workers_range:
    time_taken = benchmark_workers(large_dict, lambda x: x*2, workers)
    results[workers] = time_taken
    print(f"{workers} workers: {time_taken:.2f}s")

# Find optimal worker count
optimal_workers = min(results, key=results.get)
print(f"\nOptimal workers: {optimal_workers} (Time: {results[optimal_workers]:.2f}s)")