#!/usr/bin/env python3
"""
Test script to demonstrate the improved LLM logging functionality.
"""

import os
import sys
import json
from pathlib import Path

# Add the parent directory to the path so we can import intabular
sys.path.insert(0, str(Path(__file__).parent.parent))

# Set up test environment
os.environ['INTABULAR_LLM_LOGGING'] = 'true'
os.environ['INTABULAR_LOG_DIRECTORY'] = '.aisandbox/test_logs'
os.environ['OPENAI_API_KEY'] = 'test-key-for-logging-demo'

try:
    from intabular.core.llm_logger import log_llm_call
    from openai import OpenAI
    
    print("Testing improved LLM logging functionality...")
    
    # Mock the OpenAI client for testing
    class MockResponse:
        def __init__(self):
            self.choices = [MockChoice()]
            self.model = "gpt-4o-mini"
            self.id = "test-123"
            
        def model_dump(self):
            return {
                "id": self.id,
                "model": self.model,
                "choices": [{"message": {"content": "Test response"}}]
            }
    
    class MockChoice:
        def __init__(self):
            self.message = MockMessage()
    
    class MockMessage:
        def __init__(self):
            self.content = '{"test": "response"}'
    
    class MockClient:
        class MockChat:
            class MockCompletions:
                def create(self, **kwargs):
                    return MockResponse()
            completions = MockCompletions()
        chat = MockChat()
    
    # Test the logging
    client = MockClient()
    
    def test_function():
        """This function will be captured as the caller name"""
        test_kwargs = {
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": "This is a test prompt for logging demonstration"}],
            "temperature": 0.1,
            "max_tokens": 100
        }
        
        response = log_llm_call(
            lambda: client.chat.completions.create(**test_kwargs),
            **test_kwargs
        )
        return response
    
    # Run the test
    response = test_function()
    
    # Check if log file was created
    log_dir = Path('.aisandbox/test_logs/llm_calls')
    if log_dir.exists():
        log_files = list(log_dir.glob('*.jsonl'))
        print(f"\n✅ Log files created: {[f.name for f in log_files]}")
        
        for log_file in log_files:
            print(f"\n📄 Contents of {log_file.name}:")
            with open(log_file, 'r') as f:
                for line in f:
                    try:
                        log_entry = json.loads(line.strip())
                        print(f"  🕐 Timestamp: {log_entry.get('timestamp', 'N/A')}")
                        print(f"  📞 Caller: {log_entry.get('caller', 'N/A')}")
                        print(f"  🤖 Model: {log_entry.get('model', 'N/A')}")
                        print(f"  🌡️  Temperature: {log_entry.get('temperature', 'N/A')}")
                        print(f"  💬 Prompt: {log_entry.get('prompt', 'N/A')[:100]}...")
                        print(f"  ✨ Response available: {'Yes' if log_entry.get('response') else 'No'}")
                        print("  " + "-" * 50)
                    except json.JSONDecodeError:
                        print(f"  ⚠️ Invalid JSON line: {line}")
    else:
        print("❌ No log directory created")
        
    print("\n🎉 LLM logging test complete!")
    
except Exception as e:
    print(f"❌ Test failed: {e}")
    import traceback
    traceback.print_exc() 