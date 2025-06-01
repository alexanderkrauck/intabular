"""
Centralized logging configuration for InTabular.
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional
import json
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""
    
    def format(self, record):
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add extra fields if present
        if hasattr(record, 'prompt'):
            log_entry['prompt'] = record.prompt
        if hasattr(record, 'response'):
            log_entry['response'] = record.response
        if hasattr(record, 'strategy'):
            log_entry['strategy'] = record.strategy
        if hasattr(record, 'confidence'):
            log_entry['confidence'] = record.confidence
        if hasattr(record, 'duration'):
            log_entry['duration'] = record.duration
        if hasattr(record, 'field_name'):
            log_entry['field_name'] = record.field_name
        if hasattr(record, 'source_columns'):
            log_entry['source_columns'] = record.source_columns
        if hasattr(record, 'target_column'):
            log_entry['target_column'] = record.target_column
        
        return json.dumps(log_entry)


class ColoredFormatter(logging.Formatter):
    """Colored formatter for console output"""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green  
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        log_color = self.COLORS.get(record.levelname, '')
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    console_output: bool = True,
    json_format: bool = False
) -> logging.Logger:
    """
    Set up comprehensive logging for InTabular
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for log output
        console_output: Whether to output to console
        json_format: Whether to use JSON formatting for file output
    
    Returns:
        Configured logger instance
    """
    
    # Create root logger
    logger = logging.getLogger('intabular')
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)  # Console shows INFO and above
        
        console_format = '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
        console_formatter = ColoredFormatter(console_format)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)  # File captures everything
        
        if json_format:
            file_formatter = JSONFormatter()
        else:
            file_format = '%(asctime)s | %(levelname)-8s | %(name)s | %(module)s:%(funcName)s:%(lineno)d | %(message)s'
            file_formatter = logging.Formatter(file_format)
        
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a specific module"""
    return logging.getLogger(f'intabular.{name}')