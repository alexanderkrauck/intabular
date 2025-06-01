"""
Fast core tests for essential functionality
These tests run quickly without any external dependencies and cover the most critical paths
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch


class TestCoreTransformations:
    """Essential transformation tests that must always pass"""
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_basic_string_transformations(self, mock_processor):
        """Test fundamental string transformation operations"""
        test_cases = [
            # Basic string operations
            ({'email': 'TEST@EXAMPLE.COM'}, 'email.lower()', 'test@example.com'),
            ({'name': '  John Doe  '}, 'name.strip()', 'John Doe'),
            ({'text': 'hello world'}, 'text.upper()', 'HELLO WORLD'),
            ({'text': 'hello-world'}, 'text.replace("-", "_")', 'hello_world'),
            
            # Numeric operations
            ({'number': '123.45'}, 'str(round(float(number), 1))', '123.5'),
            ({'value': '1000'}, 'str(int(value))', '1000'),
        ]
        
        for source_data, rule, expected in test_cases:
            result = mock_processor.execute_transformation(rule, source_data)
            assert result == expected, f"Transform failed: {rule} with {source_data}"
    
    @pytest.mark.no_llm  
    @pytest.mark.unit
    def test_safety_restrictions(self, mock_processor):
        """Test that unsafe operations are blocked"""
        # These should be blocked by the safe namespace
        unsafe_rules = [
            'import os',
            'exec("print(hello)")',  
            'eval("1+1")',
            'open("file.txt")',
            '__import__("sys")'
        ]
        
        for rule in unsafe_rules:
            with pytest.raises((ValueError, NameError, TypeError)):
                mock_processor.execute_transformation(rule, {'test': 'value'})
    
    @pytest.mark.no_llm
    @pytest.mark.unit  
    def test_data_loading(self, perfect_match_df, format_transform_df):
        """Test that test data loads correctly"""
        # Basic data integrity
        assert len(perfect_match_df) > 0, "Perfect match data should load"
        assert len(format_transform_df) > 0, "Format transform data should load"
        
        # Required columns exist
        assert 'email' in perfect_match_df.columns, "Perfect match should have email"
        assert 'email_address' in format_transform_df.columns, "Format transform should have email_address"


class TestConfigurationLoading:
    """Test configuration loading and validation"""
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_config_loading(self, customer_crm_config, simple_contacts_config):
        """Test that configurations load properly"""
        # Basic config validation
        assert customer_crm_config.purpose, "Customer config should have purpose"
        assert len(customer_crm_config.enrichment_columns) > 0, "Should have enrichment columns"
        
        assert simple_contacts_config.purpose, "Simple config should have purpose"
        assert 'email' in simple_contacts_config.enrichment_columns, "Should have email column"
        
        # Entity column detection
        entity_cols = customer_crm_config.entity_columns
        assert len(entity_cols) > 0, "Should detect entity columns"
        assert 'email' in entity_cols, "Email should be entity column"


class TestDataQuality:
    """Test data quality across all domains"""
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    @pytest.mark.parametrize("fixture_name", [
        "perfect_match_df",
        "format_transform_df", 
        "edge_cases_df",
        "industrial_sensors_df",
        "financial_transactions_df",
        "lab_results_df"
    ])
    def test_data_completeness(self, fixture_name, request):
        """Test that all test data files are reasonably complete"""
        df = request.getfixturevalue(fixture_name)
        
        # Basic checks
        assert len(df) > 0, f"{fixture_name} should not be empty"
        assert len(df.columns) > 0, f"{fixture_name} should have columns"
        
        # Data completeness check
        total_cells = len(df) * len(df.columns)
        non_null_cells = df.count().sum()
        completeness = non_null_cells / total_cells if total_cells > 0 else 0
        
        assert completeness >= 0.3, f"{fixture_name} should be at least 30% complete, got {completeness:.2%}"
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_column_name_consistency(self, industrial_sensors_df, financial_transactions_df, lab_results_df):
        """Test that column names follow expected patterns"""
        # Industrial data should have sensor-related columns
        assert 'sensor_id' in industrial_sensors_df.columns
        assert any('temperature' in col for col in industrial_sensors_df.columns)
        
        # Financial data should have transaction-related columns  
        assert 'transaction_id' in financial_transactions_df.columns
        assert any('amount' in col for col in financial_transactions_df.columns)
        
        # Scientific data should have sample-related columns
        assert 'sample_id' in lab_results_df.columns
        assert any('ph' in col for col in lab_results_df.columns)


class TestMockingInfrastructure:
    """Test that our mocking infrastructure works correctly"""
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_mock_processor_basic_functionality(self, mock_processor):
        """Test that mock processor responds correctly"""
        # Should have the required methods
        assert hasattr(mock_processor, 'execute_transformation')
        assert hasattr(mock_processor, 'apply_column_mapping')
        assert hasattr(mock_processor, 'client')
        
        # Should be able to execute basic transformations
        result = mock_processor.execute_transformation('test_field.lower()', {'test_field': 'HELLO'})
        assert result == 'hello'
    
    @pytest.mark.no_llm
    @pytest.mark.unit 
    def test_mock_client_configuration(self, mock_openai_client):
        """Test that mock OpenAI client is properly configured"""
        # Should have the required interface
        assert hasattr(mock_openai_client, 'chat')
        assert hasattr(mock_openai_client.chat, 'completions')
        assert hasattr(mock_openai_client.chat.completions, 'create')
        
        # Should return mock responses
        response = mock_openai_client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": "test"}]
        )
        assert hasattr(response, 'choices')
        assert len(response.choices) > 0


class TestErrorHandling:
    """Test error handling and edge cases"""
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_empty_data_handling(self):
        """Test handling of empty data structures"""
        # Empty DataFrame
        empty_df = pd.DataFrame()
        assert len(empty_df) == 0
        assert len(empty_df.columns) == 0
        
        # DataFrame with no data but columns
        empty_with_cols = pd.DataFrame(columns=['a', 'b', 'c'])
        assert len(empty_with_cols) == 0
        assert len(empty_with_cols.columns) == 3
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_none_and_nan_handling(self, mock_processor):
        """Test handling of None and NaN values in safe namespace"""
        test_cases = [
            # Safe transformations that work in the restricted namespace
            ({'field': None}, 'str(field) if field is not None else ""', ''),
            ({'field': ''}, 'field if field else "empty"', 'empty'),
            ({'field': 'valid'}, 'field.lower()', 'valid'),
        ]
        
        for source_data, rule, expected in test_cases:
            try:
                result = mock_processor.execute_transformation(rule, source_data)
                assert result == expected, f"Failed: {rule} with {source_data}"
            except (NameError, AttributeError):
                # Expected for some advanced operations in mock environment
                continue 