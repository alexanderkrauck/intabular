"""
Tests for edge cases and error handling scenarios
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock


class TestEdgeCases:
    """Test cases for edge cases and error conditions"""
    
    def test_missing_data_handling(self, processor, customer_crm_config, edge_cases_df):
        """Test handling of missing and null data"""
        # Test with row that has missing name
        missing_name_row = edge_cases_df.iloc[2].to_dict()  # Third row has missing name
        
        # Create a simple mapping
        mapping_result = {
            'transformation_type': 'format',
            'transformation_rule': 'name.strip().lower() if name else "unknown"',
            'reasoning': 'Handle missing names'
        }
        
        result = processor.apply_column_mapping(
            mapping_result, missing_name_row, 'full_name', customer_crm_config,
            {'row_count': 1, 'column_count': len(missing_name_row)}
        )
        
        # Should handle missing data gracefully
        assert result is not None, "Should handle missing data"
    
    def test_duplicate_handling(self, edge_cases_df):
        """Test detection of duplicate entries"""
        # Edge cases CSV has duplicate emails
        email_counts = edge_cases_df['email'].value_counts()
        duplicate_emails = email_counts[email_counts > 1]
        
        assert len(duplicate_emails) > 0, "Test data should contain duplicates"
        assert 'duplicate@test.com' in duplicate_emails.index, "Should have test duplicate"
    
    def test_malformed_data_handling(self, processor, customer_crm_config):
        """Test handling of malformed data"""
        malformed_data = {
            'email': 'weird@email..com',  # Double dots
            'phone': 'not-a-phone',
            'deal_value': 'invalid-number'
        }
        
        # Test email normalization with malformed email
        email_mapping = {
            'transformation_type': 'format',
            'transformation_rule': 'email.strip().lower()',
            'reasoning': 'Normalize email'
        }
        
        result = processor.apply_column_mapping(
            email_mapping, malformed_data, 'email', customer_crm_config,
            {'row_count': 1, 'column_count': 3}
        )
        
        # Should still process, even if malformed
        assert result == 'weird@email..com', "Should handle malformed data"
    
    def test_very_large_values(self, processor, customer_crm_config):
        """Test handling of very large numeric values"""
        large_value_data = {
            'deal_value': '1000000',  # Very large deal
            'notes': 'A' * 10000  # Very long text
        }
        
        # Test large value handling
        value_mapping = {
            'transformation_type': 'format',
            'transformation_rule': 'deal_value',
            'reasoning': 'Pass through large value'
        }
        
        result = processor.apply_column_mapping(
            value_mapping, large_value_data, 'deal_value', customer_crm_config,
            {'row_count': 1, 'column_count': 2}
        )
        
        assert result == '1000000', "Should handle large values"
    
    def test_unicode_and_special_characters(self, processor, customer_crm_config):
        """Test handling of unicode and special characters"""
        unicode_data = {
            'full_name': 'José García-López',
            'company': 'Café & Co™',
            'notes': 'Special chars: àáâãäåæçèéêë 中文 🚀'
        }
        
        name_mapping = {
            'transformation_type': 'format',
            'transformation_rule': 'full_name.lower()',
            'reasoning': 'Normalize unicode name'
        }
        
        result = processor.apply_column_mapping(
            name_mapping, unicode_data, 'full_name', customer_crm_config,
            {'row_count': 1, 'column_count': 3}
        )
        
        assert 'josé' in result, "Should handle unicode characters"
        assert 'garcía' in result, "Should preserve unicode in normalization"
    
    def test_empty_string_vs_none_handling(self, processor, customer_crm_config):
        """Test distinction between empty strings and None values"""
        test_cases = [
            ({'field': ''}, 'field if field else "empty"', 'empty'),
            ({'field': None}, 'field if field else "none"', 'none'),  # None becomes empty string, then rule applies
            ({'field': '  '}, 'field.strip() if field.strip() else "whitespace"', 'whitespace')
        ]
        
        for source_data, rule, expected in test_cases:
            mapping = {
                'transformation_type': 'format',
                'transformation_rule': rule,
                'reasoning': 'Test empty vs none'
            }
            
            result = processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed for {source_data}: got {result}, expected {expected}"
    
    def test_circular_reference_prevention(self, processor, customer_crm_config):
        """Test that transformation rules can't create circular references"""
        # This should not cause infinite recursion
        safe_rule = 'email.lower()'
        source_data = {'email': 'TEST@EXAMPLE.COM'}
        
        result = processor.execute_transformation(safe_rule, source_data)
        assert result == 'test@example.com', "Should handle self-reference safely"
    
    def test_column_name_case_sensitivity(self, analyzer, customer_crm_config):
        """Test handling of different column name cases"""
        mixed_case_df = pd.DataFrame({
            'EMAIL': ['test@example.com'],
            'Full_Name': ['Test User'],
            'COMPANY_NAME': ['Test Corp']
        })
        
        # Column names should be normalized to lowercase
        analysis = analyzer.analyze_dataframe_structure(mixed_case_df, "Mixed case test")
        
        column_names = list(analysis.dataframe_column_analysis.keys())
        assert 'email' in column_names, "Should normalize EMAIL to email"
        assert 'full_name' in column_names, "Should normalize Full_Name to full_name"
        assert 'company_name' in column_names, "Should normalize COMPANY_NAME to company_name"
    
    def test_configuration_validation(self, test_data_dir):
        """Test configuration file validation"""
        from intabular.core.config import GatekeeperConfig
        
        # Test loading valid config
        valid_config_path = test_data_dir / "configs" / "customer_crm.yaml"
        config = GatekeeperConfig.from_yaml(str(valid_config_path))
        
        assert config.purpose, "Config should have a purpose"
        assert len(config.enrichment_columns) > 0, "Config should have enrichment columns"
        
        # Test invalid config path
        with pytest.raises(FileNotFoundError):
            GatekeeperConfig.from_yaml("nonexistent_config.yaml")
    
    def test_memory_efficiency_large_dataframe(self, analyzer, customer_crm_config):
        """Test memory efficiency with larger dataframes"""
        # Create a moderately large dataframe
        large_data = {
            'email': [f'user{i}@example.com' for i in range(1000)],
            'name': [f'User {i}' for i in range(1000)],
            'company': [f'Company {i}' for i in range(1000)]
        }
        large_df = pd.DataFrame(large_data)
        
        # Should be able to analyze without memory issues
        analysis = analyzer.analyze_dataframe_structure(large_df, "Large dataframe test")
        
        assert analysis.general_ingestion_analysis['row_count'] == 1000
        assert len(analysis.dataframe_column_analysis) == 3
    
    def test_concurrent_processing_safety(self, processor, customer_crm_config):
        """Test that transformations are safe for concurrent processing"""
        import threading
        import time
        
        results = []
        errors = []
        
        def worker(thread_id):
            try:
                source_data = {'email': f'user{thread_id}@example.com'}
                rule = 'email.lower().strip()'
                result = processor.execute_transformation(rule, source_data)
                results.append((thread_id, result))
            except Exception as e:
                errors.append((thread_id, str(e)))
        
        # Create multiple threads
        threads = []
        for i in range(10):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check results
        assert len(errors) == 0, f"Concurrent processing errors: {errors}"
        assert len(results) == 10, "All threads should complete successfully"
    
    def test_nan_and_inf_handling(self, processor, customer_crm_config):
        """Test handling of NaN and infinity values"""
        problematic_data = {
            'number_field': np.nan,
            'inf_field': np.inf,
            'neg_inf_field': -np.inf,
            'text_field': 'normal_text'
        }
        
        # Test that NaN becomes empty string
        result = processor.execute_transformation('number_field', problematic_data)
        assert result == '', "NaN should become empty string"
        
        # Test that inf becomes string representation
        result = processor.execute_transformation('inf_field', problematic_data)
        assert 'inf' in result.lower(), "Infinity should be converted to string"
    
    def test_extremely_long_text_handling(self, mock_processor, customer_crm_config):
        """Test handling of extremely long text content"""
        # Create very long text
        very_long_text = 'A' * 100000  # 100k characters
        
        source_data = {'notes': very_long_text}
        mapping_result = {
            'transformation_type': 'llm_format',
            'reasoning': 'Process very long text'
        }
        
        # Mock the LLM response
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "Processed long text"
        
        # Clear any existing side_effect and set return_value
        mock_processor.client.chat.completions.create.side_effect = None
        mock_processor.client.chat.completions.create.return_value = mock_response
        
        # Should handle long text without errors
        result = mock_processor.apply_column_mapping(
            mapping_result, source_data, 'notes', customer_crm_config,
            {'row_count': 1, 'column_count': 1}
        )
        
        assert result == "Processed long text", "Should handle very long text" 