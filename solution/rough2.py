import threading
from queue import Queue
from typing import Callable, Any, Union

def transform_dict_string_leaves(
    d: dict, 
    func: Callable[[str], str], 
    max_workers: int = 10
) -> dict:
    """
    Creates a transformed copy of a nested dictionary with string leaves modified by func.
    Non-dict and non-string values are copied by reference (shallow copy).
    
    Args:
        d: Input dictionary (nested structure)
        func: Transformation function for string values
        max_workers: Number of worker threads
        
    Returns:
        New dictionary with transformed string leaves
    """
    # Create root of new dictionary structure
    new_root = {}
    task_queue = Queue()
    task_queue.put((d, new_root))  # (original_dict, new_dict_parent)

    # Worker function to process dictionaries
    def worker():
        while True:
            task = task_queue.get()
            if task is None:  # Termination signal
                task_queue.task_done()
                break
                
            original_dict, new_dict = task
            for key, value in original_dict.items():
                if isinstance(value, dict):
                    # Create new nested dict and enqueue for processing
                    new_nested = {}
                    new_dict[key] = new_nested
                    task_queue.put((value, new_nested))
                elif isinstance(value, str):
                    # Apply transformation to string leaves
                    new_dict[key] = func(value)
                else:
                    # Shallow copy for non-dict/non-string values
                    new_dict[key] = value
            task_queue.task_done()

    # Create and start worker threads
    workers = []
    for _ in range(max_workers):
        t = threading.Thread(target=worker)
        t.start()
        workers.append(t)

    # Wait until all dictionaries are processed
    task_queue.join()

    # Send termination signals to workers
    for _ in range(max_workers):
        task_queue.put(None)
        
    # Wait for all workers to finish
    for t in workers:
        t.join()

    return new_root

if __name__ == "__main__":
    # Test case
    original_dict = {
        'level1a': {
            'level2a': {f'level3a{i}': f'value{i}' for i in range(1000)},
            'level2b': 'root_value'
        },
        'level1b': {f'level2c{i}': {f'level3b{j}': f'nested{j}' for j in range(50)} 
                    for i in range(200)}
    }
    
    # Transform without modifying original
    transformed = transform_dict_string_leaves(
        original_dict, 
        lambda x: x*2,
        max_workers=10
    )
    
    print("Original:", original_dict)
    print("Transformed:", transformed)
    # Verify original unchanged
    assert original_dict['level1a']['level2b'] == 'root_value'
    assert transformed['level1a']['level2b'] == 'root_valueroot_value'