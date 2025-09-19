"""
Parallel processing utilities for I/O-bound operations.
"""

from typing import Any, Callable, List
from pydantic import validate_call


@validate_call
def map_func(func: Callable, items: List[Any]) -> List[Any]:
    """
    Apply a function to a list of items in parallel using ThreadPoolExecutor.

    This is like map() but parallel - perfect for I/O-bound operations.

    Args:
        func: Function to apply to each item
        items: List of items to process

    Returns:
        List of results in the same order as input items

    Example:
        def process_item(item):
            # Some I/O-bound work
            return f"processed_{item}"

        results = map_func(process_item, ["a", "b", "c"])
        # Returns: ["processed_a", "processed_b", "processed_c"]
    """
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor() as executor:
        return list(executor.map(func, items))
