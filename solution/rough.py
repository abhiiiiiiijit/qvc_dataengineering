from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

def update_dict_string_leaves(d: dict, func: Callable[[str], str], max_workers: int = 10) -> None:
    """
    Updates all string leaves in a nested dictionary in-place using parallel processing.
    
    Args:
        d: Nested dictionary to update
        func: Function to apply to string values
        max_workers: Number of worker threads (default=10)
        
    Returns:
        Updated dictionary (modified in-place)
    """
    q = Queue()
    q.put(d)  # Start with root dictionary

    def worker():
        """Process dictionaries from the queue"""
        while True:
            item = q.get()
            if item is None:  # Termination signal
                break
                
            nested_dicts = []
            for key, value in item.items():
                if isinstance(value, dict):
                    nested_dicts.append(value)
                elif isinstance(value, str):
                    item[key] = func(value)  # Update in-place
            
            # Add nested dictionaries to queue
            for nd in nested_dicts:
                q.put(nd)
                
            q.task_done()

    # Create worker threads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Start worker tasks
        workers = [executor.submit(worker) for _ in range(max_workers)]
        
        # Wait until all dictionaries are processed
        q.join()
        
        # Send termination signals to workers
        for _ in range(max_workers):
            q.put(None)
            
        # Wait for workers to exit
        for w in workers:
            w.result()



if __name__ == "__main__":
    # Test with large dictionary
    d = {
        'level1a': {
            'level2a': {f'level3a{i}': f'value{i}' for i in range(1000)},
            'level2b': 'root_value'
        },
        'level1b': {f'level2c{i}': {f'level3b{j}': f'nested{j}' for j in range(50)} 
                    for i in range(200)}
    }
    
    print("Original dictionary:", d)
    # Update function (e.g., string transformation)
    update_dict_string_leaves(d, lambda x: x*2, max_workers=20)

    print("Updated dictionary:", d)