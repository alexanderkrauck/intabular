"""
Core utility functions for intabular.
"""

import time
from typing import Callable, Iterable, List
from concurrent.futures import ThreadPoolExecutor, as_completed


def parallel_map(func: Callable, items: Iterable, max_workers: int = 5, timeout: int = 30, retries: int = 0) -> List:
    """
    Apply function to iterable items in parallel with optional retry support.
    
    Args:
        func: Function to apply to each item 
        items: Iterable of items to process
        max_workers: Maximum parallel workers
        timeout: Timeout per item in seconds
        retries: Number of retry attempts on failure (0 = no retries)
        
    Returns:
        List of func(item) results in same order as input
        
    Raises:
        Exception: If any item fails after all retry attempts
    """
    items_list = list(items)  # Convert to list to preserve order
    results = [None] * len(items_list)
    
    def _retry_wrapper(item):
        """Wrapper that handles retries for individual function calls"""
        last_exception = None
        
        for attempt in range(retries + 1):  # +1 because retries=0 means 1 attempt
            try:
                return func(item)
            except Exception as e:
                last_exception = e
                if attempt < retries:  # Don't sleep on final attempt
                    # Exponential backoff: 1s, 2s, 4s, etc.
                    sleep_time = 2 ** attempt
                    time.sleep(sleep_time)
                    continue
                else:
                    # All attempts exhausted, re-raise the last exception
                    raise last_exception
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks with their index
        future_to_index = {
            executor.submit(_retry_wrapper, item): idx 
            for idx, item in enumerate(items_list)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_index, timeout=timeout * len(items_list)):
            idx = future_to_index[future]
            try:
                results[idx] = future.result(timeout=timeout)
            except Exception as e:
                raise Exception(f"Failed processing item {idx} ({items_list[idx]}) after {retries + 1} attempts: {e}")
    
    return results 