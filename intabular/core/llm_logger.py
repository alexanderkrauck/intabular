"""
Simple LLM call logging utility for debugging and analysis.
"""

import os
import json
import inspect
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Callable


def log_llm_call(call_func: Callable[[], Any], **kwargs) -> Any:
    """
    Log LLM calls to separate files when LLM logging is enabled.
    
    Args:
        call_func: Function that makes the LLM call (e.g., lambda: client.chat.completions.create(**kwargs))
        **kwargs: The arguments being passed to the LLM call (for logging purposes)
        
    Returns:
        The response from the LLM call
    """
    
    # Check if LLM logging is enabled
    llm_logging_enabled = os.getenv('INTABULAR_LLM_LOGGING', 'false').lower() == 'true'
    
    # Make the actual LLM call
    response = call_func()
    
    # Log if enabled
    if llm_logging_enabled:
        _log_llm_call_details(kwargs, response)
    
    return response


def _log_llm_call_details(call_kwargs: Dict[str, Any], response: Any):
    """Log the details of an LLM call to a file."""
    
    try:
        # Get calling function name - go up levels to skip lambda and log functions
        frame = inspect.currentframe()
        caller_name = "unknown"
        try:
            # Go through the stack to find the actual calling function
            current_frame = frame.f_back
            for i in range(6):  # Go up several levels to find the real caller
                if current_frame and current_frame.f_code.co_name not in ['<lambda>', 'log_llm_call', '_log_llm_call_details']:
                    caller_name = current_frame.f_code.co_name
                    break
                if current_frame:
                    current_frame = current_frame.f_back
        finally:
            del frame
        
        # Create log directory
        log_dir = os.getenv('INTABULAR_LOG_DIRECTORY', 'logs')
        llm_log_dir = Path(log_dir) / 'llm_calls'
        llm_log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create log file path
        log_file = llm_log_dir / f"{caller_name}.jsonl"
        
        # Extract information from kwargs
        prompt_content = "Prompt not provided"
        model_used = call_kwargs.get('model', 'Unknown')
        temperature = call_kwargs.get('temperature', 'Unknown')
        
        # Extract prompt from messages
        messages = call_kwargs.get('messages', [])
        if messages and len(messages) > 0:
            user_message = next((msg for msg in messages if msg.get('role') == 'user'), None)
            if user_message:
                content = user_message.get('content', '')
                prompt_content = content[:1000] + "..." if len(content) > 1000 else content
        
        # Prepare log entry
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "caller": caller_name,
            "model": model_used,
            "temperature": temperature,
            "prompt": prompt_content,
            "full_request": _sanitize_for_json(call_kwargs),
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