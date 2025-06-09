from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from typing import Callable
import threading

#part1 inplace transformation
def update_dict_string_leaves(d: dict, func: Callable, max_workers: int = 10) -> None:
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

#part 2 non-mutating transformation
def update_dict_string_leaves_non_mutating(
    d: dict, 
    func: Callable, 
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
                else:
                    # Shallow copy for non-dict/non-string values
                    new_dict[key] = func(value)
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

#Q1
def double_string(val: str) -> str:
    if isinstance(val, str):
        return val * 2
    else:
        return val
#Q3
def generalise_to_double_list(val) :
    if isinstance(val, list) or isinstance(val, str):
        return val * 2
    else:
        return val

if __name__ == "__main__":
    d = {
    'key11': 
        {
            'key21': 'a',
            'key22': 'b',
        },
    'key12': 'c',
        
    'key13': 
            {
                'key31': 'd',
                'key32': 
                    {
                        'key21': 'e',
                        'key22': 'f',
                    },
            },
    }

    d2 = {
    'key11': 
        {
            'key21': 'a',
            'key22': 'b',
        },
    'key12': 'c',
        
    'key13': 
            {
                'key31': 'd',
                'key32': 
                    {
                        'key21': 'e',
                        'key22': 'f',
                    },
            },
    'key14': [1,2,3], 
    1 : (2,3), 
}

    
    print("Original dictionary d:", d)
# Q2.1 Update string values 'a' -> 'aa'
    update_dict_string_leaves(d, double_string, max_workers=4)
    print("Updated dictionary inplace 2.1:", d)
# Q2.2 Non-mutating version
    new_d_2_2 = update_dict_string_leaves_non_mutating(d, double_string, max_workers=4)
    print("Dictonery after result 2.1", d)
    print("New dictionary with no mutation 2.2", new_d_2_2)

    print("Original dictionary d2:", d2)
# Q3.1 Update string and list values 
    update_dict_string_leaves(d2, generalise_to_double_list, max_workers=4)
    print("Updated dictionary inplace updating 3.1:", d2)
# Q3.2 Non-mutating version of above
    new_d_3_2 = update_dict_string_leaves_non_mutating(d2, generalise_to_double_list, max_workers=4)
    print("Dictionary after result 3.1:", d2)
    print("New dictionary with doubled strings n list 3.2:", new_d_3_2)