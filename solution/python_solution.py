from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

#part1 of question 2 and 3 inplace transformation
def update_dict_string_leaves(d: dict, func: Callable, max_workers: int = 4) -> None:
    
    task_queue = Queue()
    task_queue.put(d)  # Start with root dictionary

    def worker():
        #Process dictionaries from the queue
        while True:
            item = task_queue.get()
            if item is None:  # Termination signal
                break
                
            nested_dicts = []
            for key, value in item.items():
                if isinstance(value, dict):
                    nested_dicts.append(value)
                else:
                    item[key] = func(value)  # Update 
            
            # Add nested dictionaries to queue
            for nd in nested_dicts:
                task_queue.put(nd)
                
            task_queue.task_done()

    # Create worker threads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Start worker tasks
        workers = [executor.submit(worker) for _ in range(max_workers)]
        
        # Wait until all dictionaries are processed
        task_queue.join()
        
        # Send termination signals to workers
        for _ in range(max_workers):
            task_queue.put(None)
            
        # Wait for workers to exit
        for w in workers:
            w.result()


#part 2 non-mutating transformation
def update_dict_string_leaves_non_mutating(d: dict, func: Callable, max_workers: int = 4) -> dict:

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
                    # Apply transformation to string leaves or copy other values
                    new_dict[key] = func(value)
            task_queue.task_done()

    # Create worker threads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Start worker tasks
        workers = [executor.submit(worker) for _ in range(max_workers)]
        
        # Wait until all dictionaries are processed
        task_queue.join()
        
        # Send termination signals to workers
        for _ in range(max_workers):
            task_queue.put(None)
            
        # Wait for workers to exit
        for w in workers:
            w.result()

    return new_root


#Q1 check if the value is a string and double it
def double_string(val: str) -> str:
    if isinstance(val, str):
        return val * 2
    else:
        return val
    
#Q3 check if the value is a list or string and double it
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

#Ans1: To transform a dictionary, I used threads to update values concurrently for efficiency considering highly nested dictionaries.
#   A function that handles multi-threading and another one updates string leaves in a dictionary.


    print("Original dictionary d:", d)
# Q2.1 Update string values 'a' -> 'aa'
    update_dict_string_leaves(d, double_string, max_workers=4) #2 or 4 max_workers works well for my system for large dictionaries
    print("Updated dictionary inplace 2.1:", d)
# Q2.2 Non-mutating version
    new_d_2_2 = update_dict_string_leaves_non_mutating(d, double_string, max_workers=4)
    print("Dictonery after result 2.1", d)
    print("New dictionary with no mutation 2.2", new_d_2_2)

    print("Original dictionary d2:", d2)
# Q3.1 Updates string and list values 
    update_dict_string_leaves(d2, generalise_to_double_list, max_workers=4)
    print("Updated dictionary inplace updating 3.1:", d2)
# Q3.2 Non-mutating version of above
    new_d_3_2 = update_dict_string_leaves_non_mutating(d2, generalise_to_double_list, max_workers=4)
    print("Dictionary after result 3.1:", d2)
    print("New dictionary with doubled strings n list 3.2:", new_d_3_2)