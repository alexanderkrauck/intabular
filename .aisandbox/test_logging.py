#!/usr/bin/env python3
"""
Test script to verify the comprehensive logging system.
"""

import os
import sys
from pathlib import Path

# Add intabular to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Set up logging environment variables for testing
os.environ['INTABULAR_LOG_FILE'] = '.aisandbox/test_logs/intabular_test.log'
os.environ['INTABULAR_LOG_LEVEL'] = 'DEBUG'
os.environ['INTABULAR_LOG_JSON'] = 'true'

def test_logging_system():
    """Test the comprehensive logging system"""
    
    print("🧪 Testing InTabular Logging System")
    print("=" * 50)
    
    # Test basic logging setup
    print("\n📋 Testing basic logging setup...")
    try:
        from intabular.core.logging_config import setup_logging, get_logger
        
        # Set up logging with all features
        setup_logging(
            level="DEBUG",
            log_file=".aisandbox/test_logs/detailed_test.log",
            console_output=True,
            json_format=False
        )
        
        logger = get_logger('test')
        print("✅ Basic logging setup successful!")
        
        # Test different log levels
        logger.debug("🔍 Debug message test")
        logger.info("ℹ️  Info message test")
        logger.warning("⚠️ Warning message test")
        logger.error("❌ Error message test")
        
        print("✅ Log level tests completed!")
        
    except Exception as e:
        print(f"❌ Basic logging test failed: {e}")
        return False
    
    # Test structured logging
    print("\n📊 Testing structured logging...")
    try:
        from intabular.core.logging_config import log_prompt_response, log_strategy_creation, log_field_processing
        
        # Test prompt/response logging
        log_prompt_response(
            logger, 
            "Test prompt for analysis", 
            "Test response from LLM",
            model="gpt-4o-mini",
            duration=1.23
        )
        
        # Test strategy creation logging  
        log_strategy_creation(
            logger,
            "email",
            "replace", 
            0.95,
            ["contact_email", "email_address"]
        )
        
        # Test field processing logging
        log_field_processing(
            logger,
            "full_name",
            "prompt_merge",
            True
        )
        
        print("✅ Structured logging tests completed!")
        
    except Exception as e:
        print(f"❌ Structured logging test failed: {e}")
        return False
    
    # Test module-specific loggers
    print("\n🔧 Testing module-specific loggers...")
    try:
        from intabular.core.logging_config import get_logger
        
        # Test different module loggers
        analyzer_logger = get_logger('analyzer')
        strategy_logger = get_logger('strategy')
        processor_logger = get_logger('processor')
        
        analyzer_logger.info("📊 Analyzer module test")
        strategy_logger.info("🧠 Strategy module test")
        processor_logger.info("⚙️ Processor module test")
        
        print("✅ Module-specific logger tests completed!")
        
    except Exception as e:
        print(f"❌ Module logger test failed: {e}")
        return False
    
    # Test log file creation
    print("\n📁 Testing log file creation...")
    try:
        log_dir = Path(".aisandbox/test_logs")
        
        # Check if log files were created
        log_files = list(log_dir.glob("*.log"))
        if log_files:
            print(f"✅ Log files created: {[f.name for f in log_files]}")
            
            # Check file content
            for log_file in log_files:
                if log_file.stat().st_size > 0:
                    print(f"✅ {log_file.name} has content ({log_file.stat().st_size} bytes)")
                else:
                    print(f"⚠️ {log_file.name} is empty")
        else:
            print("⚠️ No log files found")
            
    except Exception as e:
        print(f"❌ Log file test failed: {e}")
        return False
    
    # Test JSON formatting
    print("\n🔄 Testing JSON log formatting...")
    try:
        json_logger = setup_logging(
            level="INFO",
            log_file=".aisandbox/test_logs/json_test.log",
            console_output=False,
            json_format=True
        )
        
        test_logger = get_logger('json_test')
        test_logger.info("JSON format test message", 
                        extra={
                            'test_field': 'test_value',
                            'number_field': 42,
                            'boolean_field': True
                        })
        
        print("✅ JSON formatting test completed!")
        
    except Exception as e:
        print(f"❌ JSON formatting test failed: {e}")
        return False
    
    print("\n" + "=" * 50)
    print("🎉 All logging tests passed successfully!")
    return True


if __name__ == "__main__":
    success = test_logging_system()
    
    if success:
        print("\n📋 Log files created in .aisandbox/test_logs/")
        print("📊 Check the files to verify detailed logging output")
    else:
        print("\n❌ Some tests failed - check error messages above")
        sys.exit(1) 