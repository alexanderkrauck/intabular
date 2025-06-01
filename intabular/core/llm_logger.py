"""
Simple LLM call logging utility for debugging and analysis.
"""

import os
import json
import inspect
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Callable


def log_llm_call(call_func: Callable[[], Any]) -> Any:
    """
    Log LLM calls to separate files when LLM logging is enabled.
    
    Args:
        call_func: Function that makes the LLM call (e.g., lambda: client.chat.completions.create(**kwargs))
        
    Returns:
        The response from the LLM call
    """
    
    # Check if LLM logging is enabled
    llm_logging_enabled = os.getenv('INTABULAR_LLM_LOGGING', 'false').lower() == 'true'
    
    # Make the actual LLM call
    response = call_func()
    
    # Log if enabled
    if llm_logging_enabled:
        _log_llm_call_details(call_func, response)
    
    return response


def _log_llm_call_details(call_func: Callable, response: Any):
    """Log the details of an LLM call to a file."""
    
    try:
        # Get calling function name for the log file
        frame = inspect.currentframe()
        caller_name = "unknown"
        try:
            # Go up the stack to find the actual calling function (skip this function and log_llm_call)
            caller_frame = frame.f_back.f_back.f_back
            if caller_frame:
                caller_name = caller_frame.f_code.co_name
        finally:
            del frame
        
        # Create log directory
        log_dir = os.getenv('INTABULAR_LOG_DIRECTORY', 'logs')
        llm_log_dir = Path(log_dir) / 'llm_calls'
        llm_log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create log file path
        log_file = llm_log_dir / f"{caller_name}.jsonl"
        
        # Extract kwargs from the lambda if possible (for better logging)
        call_info = "LLM call"
        try:
            # Try to get some info about the call from the function
            if hasattr(call_func, '__code__'):
                call_info = f"LLM call from {call_func.__code__.co_name}"
        except:
            pass
        
        # Prepare log entry
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "caller": caller_name,
            "call_info": call_info,
            "response": _sanitize_for_json(response.model_dump() if hasattr(response, 'model_dump') else str(response))
        }
        
        # Append to log file
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
            
    except Exception as e:
        # Don't let logging errors break the main functionality
        pass


def _sanitize_for_json(obj: Any) -> Any:
    """Convert objects to JSON-serializable format."""
    if hasattr(obj, 'model_dump'):
        return obj.model_dump()
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    elif isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    elif isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    else:
        return str(obj) 