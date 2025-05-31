"""
Core utility functions for intabular.
"""

from typing import Callable, Iterable, List
from concurrent.futures import ThreadPoolExecutor, as_completed


def parallel_map(func: Callable, items: Iterable, max_workers: int = 5, timeout: int = 30) -> List:
    """
    Apply function to iterable items in parallel.
    
    Args:
        func: Function to apply to each item 
        items: Iterable of items to process
        max_workers: Maximum parallel workers
        timeout: Timeout per item in seconds
        
    Returns:
        List of func(item) results in same order as input
    """
    items_list = list(items)  # Convert to list to preserve order
    results = [None] * len(items_list)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks with their index
        future_to_index = {
            executor.submit(func, item): idx 
            for idx, item in enumerate(items_list)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_index, timeout=timeout * len(items_list)):
            idx = future_to_index[future]
            try:
                results[idx] = future.result(timeout=timeout)
            except Exception as e:
                raise Exception(f"Failed processing item {idx} ({items_list[idx]}): {e}")
    
    return results 