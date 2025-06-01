"""
Tests for perfect column matching scenarios where source and target columns align exactly
"""

import pytest
import pandas as pd


class TestPerfectMatching:
    """Test cases for perfect column name matches"""
    
    def test_perfect_match_strategy_creation(self, analyzer, strategy_creator, customer_crm_config, perfect_match_df):
        """Test that perfect matches create appropriate format strategies"""
        # Analyze the perfect match dataframe
        analysis = analyzer.analyze_dataframe_structure(perfect_match_df, "Perfect match test data")
        
        # Create strategy
        strategy = strategy_creator.create_ingestion_strategy(customer_crm_config, analysis)
        
        # All entity columns should have some transformation type (not 'none')
        entity_mappings = strategy.no_merge_column_mappings
        for col_name, mapping in entity_mappings.items():
            if col_name in customer_crm_config.entity_columns:
                assert mapping['transformation_type'] in ['format', 'llm_format'], \
                    f"Entity column {col_name} should have a valid transformation"
        
        # Check that at least some columns are mapped
        mapped_columns = [col for col, mapping in entity_mappings.items() 
                         if mapping['transformation_type'] != 'none']
        assert len(mapped_columns) > 0, "At least some columns should be mapped"
    
    def test_perfect_match_processing(self, processor, customer_crm_config, perfect_match_df, empty_target_df):
        """Test processing with perfect column matches"""
        # Create a simple mock strategy for perfect matches
        from intabular.core.strategy import DataframeIngestionStrategyResult
        
        # Simple format transformations for perfect matches
        no_merge_mappings = {
            'email': {
                'transformation_type': 'format',
                'transformation_rule': 'email.strip().lower()',
                'reasoning': 'Direct email mapping'
            },
            'full_name': {
                'transformation_type': 'format', 
                'transformation_rule': 'full_name.strip().lower()',
                'reasoning': 'Direct name mapping'
            }
        }
        
        merge_mappings = {
            'company_name': {
                'transformation_type': 'format',
                'transformation_rule': 'company_name.strip().lower()',
                'reasoning': 'Direct company mapping'
            },
            'notes': {
                'transformation_type': 'format',
                'transformation_rule': 'notes',
                'reasoning': 'Direct notes mapping'
            }
        }
        
        strategy = DataframeIngestionStrategyResult(no_merge_mappings, merge_mappings)
        
        # Execute ingestion
        result_df = processor.execute_ingestion(
            perfect_match_df, empty_target_df, strategy, customer_crm_config, 
            {"row_count": len(perfect_match_df), "column_count": len(perfect_match_df.columns)}
        )
        
        # Verify results
        assert len(result_df) == len(perfect_match_df), "All rows should be processed"
        assert 'email' in result_df.columns, "Email column should exist"
        assert 'full_name' in result_df.columns, "Full name column should exist"
        
        # Check that email normalization worked
        assert result_df['email'].iloc[0] == 'john.doe@acme.com', "Email should be normalized"
        assert result_df['full_name'].iloc[0] == 'john doe', "Name should be normalized"
    
    def test_empty_dataframe_handling(self, analyzer, customer_crm_config):
        """Test handling of empty dataframes"""
        empty_df = pd.DataFrame()
        
        with pytest.raises(Exception):  # Should raise UnclearAssumptionsException
            analyzer.analyze_dataframe_structure(empty_df, "Empty dataframe")
    
    def test_single_row_dataframe(self, analyzer, customer_crm_config):
        """Test handling of single row dataframes"""
        single_row_df = pd.DataFrame({
            'email': ['test@example.com'],
            'name': ['Test User']
        })
        
        # Should work without issues
        analysis = analyzer.analyze_dataframe_structure(single_row_df, "Single row test")
        assert analysis.general_ingestion_analysis['row_count'] == 1
        assert 'email' in analysis.dataframe_column_analysis
    
    def test_column_name_normalization(self, analyzer, customer_crm_config):
        """Test that column names are properly normalized"""
        df_with_special_chars = pd.DataFrame({
            'Email Address!': ['test@example.com'],
            'Full Name (Person)': ['Test User'],
            'Company-Name': ['Test Corp']
        })
        
        analysis = analyzer.analyze_dataframe_structure(df_with_special_chars, "Special chars test")
        
        # Column names should be normalized to lowercase with underscores
        column_names = list(analysis.dataframe_column_analysis.keys())
        assert 'email_address_' in column_names
        assert 'full_name__person_' in column_names
        assert 'company_name' in column_names
    
    def test_all_columns_present(self, perfect_match_df, customer_crm_config):
        """Test that perfect match CSV has all expected columns"""
        expected_columns = set(customer_crm_config.all_columns.keys())
        actual_columns = set(perfect_match_df.columns)
        
        assert expected_columns.issubset(actual_columns), \
            f"Missing columns: {expected_columns - actual_columns}"
    
    def test_data_types_preserved(self, processor, customer_crm_config):
        """Test that data types are properly handled during transformation"""
        test_data = {
            'email': ['test@example.com'],
            'full_name': ['Test User'],
            'deal_value': [50000],  # Numeric value
            'notes': ['Test notes']
        }
        test_df = pd.DataFrame(test_data)
        
        # Test execute_transformation directly
        result = processor.execute_transformation('deal_value', test_data)
        assert result == '50000', "Numeric values should be converted to strings"
        
        # Test with None values
        test_data_with_none = test_data.copy()
        test_data_with_none['deal_value'] = None
        result_none = processor.execute_transformation('deal_value', test_data_with_none)
        assert result_none == '', "None values should become empty strings" 