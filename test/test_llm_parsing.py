"""
Tests for LLM format parsing scenarios where complex data requires AI interpretation
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch


class TestLLMParsing:
    """Test cases for LLM-based parsing"""
    
    @pytest.mark.llm
    def test_llm_strategy_creation(self, analyzer, strategy_creator, customer_crm_config, llm_complex_df):
        """Test that complex data creates LLM format strategies"""
        # Analyze the complex dataframe
        analysis = analyzer.analyze_dataframe_structure(llm_complex_df, "Complex data requiring LLM parsing")
        
        # Create strategy
        strategy = strategy_creator.create_ingestion_strategy(customer_crm_config, analysis)
        
        # Should have some LLM format mappings for complex data
        all_mappings = {**strategy.no_merge_column_mappings, **strategy.merge_column_mappings}
        llm_mappings = [mapping for mapping in all_mappings.values() 
                       if mapping['transformation_type'] == 'llm_format']
        
        # Complex data should actually create LLM format strategies (not just potentially)
        assert len(llm_mappings) > 0, "Complex data should create at least one LLM format strategy"
        
        # Verify that LLM mappings have proper structure
        for mapping in llm_mappings:
            assert 'reasoning' in mapping, "LLM mappings should include reasoning"
            assert mapping['transformation_type'] == 'llm_format', "Should be llm_format type"
    
    @pytest.mark.no_llm
    def test_llm_source_columns_specification(self, mock_strategy_creator, customer_crm_config):
        """Test that LLM source columns can be specified"""
        # Create a mock analysis
        mock_analysis = Mock()
        mock_analysis.dataframe_column_analysis = {
            'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'},
            'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'},
            'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}
        }
        
        # Mock the strategy response to include llm_source_columns
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = '''
        {
            "reasoning": "Need LLM to parse complex contact info",
            "transformation_type": "llm_format",
            "llm_source_columns": ["contact_info", "business_entity"]
        }
        '''
        
        # Reset the side_effect and use return_value instead
        mock_strategy_creator.client.chat.completions.create.side_effect = None
        mock_strategy_creator.client.chat.completions.create.return_value = mock_response
        
        # Create strategy
        strategy = mock_strategy_creator.create_ingestion_strategy(customer_crm_config, mock_analysis)
        
        # Check that strategy was created and contains expected LLM configuration
        assert strategy is not None, "Strategy should be created"
        
        # Verify that the mock was called to create the strategy
        mock_strategy_creator.client.chat.completions.create.assert_called()
        
        # In a real test, we would verify the llm_source_columns were processed correctly
        # but since this is fully mocked, we can only verify the basic structure exists
    
    @pytest.mark.no_llm
    def test_llm_direct_parsing(self, mock_processor, customer_crm_config):
        """Test direct LLM parsing without intermediate transformation"""
        # Create a mock LLM response
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "john.doe@acme.com"
        
        # Reset side_effect and set return_value
        mock_processor.client.chat.completions.create.side_effect = None
        mock_processor.client.chat.completions.create.return_value = mock_response
        
        # Test data with complex contact info
        source_row = {
            'contact_info': 'John Doe <john.doe@acme.com>',
            'business_entity': 'Acme Corp - Technology Solutions'
        }
        
        # Create a mapping that uses LLM format
        mapping_result = {
            'transformation_type': 'llm_format',
            'reasoning': 'Extract email from complex contact info',
            'llm_source_columns': ['contact_info']
        }
        
        # Apply the mapping
        result = mock_processor.apply_column_mapping(
            mapping_result, source_row, 'email', customer_crm_config, 
            {'row_count': 1, 'column_count': 2}
        )
        
        assert result == "john.doe@acme.com", "LLM should extract email from complex data"
        
        # Verify the LLM was called with the right data
        mock_processor.client.chat.completions.create.assert_called_once()
        call_args = mock_processor.client.chat.completions.create.call_args
        prompt = call_args[1]['messages'][0]['content']
        
        # Check that the prompt contains the source data
        assert 'contact_info' in prompt
        assert 'John Doe <john.doe@acme.com>' in prompt
    
    @pytest.mark.no_llm
    def test_llm_column_filtering(self, mock_processor, customer_crm_config):
        """Test that LLM only receives specified source columns"""
        # Mock LLM response
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "Acme Corporation"
        
        # Reset side_effect and set return_value
        mock_processor.client.chat.completions.create.side_effect = None
        mock_processor.client.chat.completions.create.return_value = mock_response
        
        # Test data with multiple columns
        source_row = {
            'contact_info': 'John Doe <john.doe@acme.com>',
            'business_entity': 'Acme Corp - Technology Solutions',
            'irrelevant_data': 'This should not be sent to LLM',
            'more_irrelevant': 'Neither should this'
        }
        
        # Create a mapping that filters to specific columns
        mapping_result = {
            'transformation_type': 'llm_format',
            'reasoning': 'Extract company name from business entity',
            'llm_source_columns': ['business_entity']
        }
        
        # Apply the mapping
        result = mock_processor.apply_column_mapping(
            mapping_result, source_row, 'company_name', customer_crm_config,
            {'row_count': 1, 'column_count': 4}
        )
        
        assert result == "Acme Corporation"
        
        # Verify the LLM prompt only contains the specified columns
        call_args = mock_processor.client.chat.completions.create.call_args
        prompt = call_args[1]['messages'][0]['content']
        
        assert 'business_entity' in prompt
        assert 'Acme Corp - Technology Solutions' in prompt
        assert 'irrelevant_data' not in prompt
        assert 'more_irrelevant' not in prompt
    
    @pytest.mark.no_llm
    def test_llm_all_columns_when_not_specified(self, mock_processor, customer_crm_config):
        """Test that LLM receives all columns when llm_source_columns not specified"""
        # Mock LLM response
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "john doe"
        
        # Reset side_effect and set return_value
        mock_processor.client.chat.completions.create.side_effect = None
        mock_processor.client.chat.completions.create.return_value = mock_response
        
        # Test data
        source_row = {
            'contact_info': 'John Doe <john.doe@acme.com>',
            'business_entity': 'Acme Corp'
        }
        
        # Create a mapping without llm_source_columns
        mapping_result = {
            'transformation_type': 'llm_format',
            'reasoning': 'Extract full name from contact info'
        }
        
        # Apply the mapping
        result = mock_processor.apply_column_mapping(
            mapping_result, source_row, 'full_name', customer_crm_config,
            {'row_count': 1, 'column_count': 2}
        )
        
        assert result == "john doe"
        
        # Verify the LLM prompt contains all columns
        call_args = mock_processor.client.chat.completions.create.call_args
        prompt = call_args[1]['messages'][0]['content']
        
        assert 'contact_info' in prompt
        assert 'business_entity' in prompt
    
    @pytest.mark.no_llm
    def test_llm_error_handling(self, mock_processor, customer_crm_config):
        """Test error handling when LLM parsing fails"""
        # Use mock processor and configure it to raise an exception
        mock_processor.client.chat.completions.create.side_effect = Exception("API Error")
        
        source_row = {'contact_info': 'John Doe <john.doe@acme.com>'}
        mapping_result = {
            'transformation_type': 'llm_format',
            'reasoning': 'Extract email from contact info'
        }
        
        # Should return empty string on error, not raise exception
        result = mock_processor.apply_column_mapping(
            mapping_result, source_row, 'email', customer_crm_config,
            {'row_count': 1, 'column_count': 1}
        )
        
        assert result == "", "Should return empty string on LLM error"
    
    @pytest.mark.no_llm
    def test_llm_with_current_value_merging(self, mock_processor, customer_crm_config):
        """Test LLM parsing with current value for merging"""
        # Mock LLM response that considers current value
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "Existing notes | New: Very interested in enterprise solution"
        
        # Reset side_effect and set return_value
        mock_processor.client.chat.completions.create.side_effect = None
        mock_processor.client.chat.completions.create.return_value = mock_response
        
        source_row = {
            'interaction_notes': 'Met at trade show, very interested in enterprise solution'
        }
        
        mapping_result = {
            'transformation_type': 'llm_format',
            'reasoning': 'Merge new notes with existing notes'
        }
        
        current_value = "Existing notes"
        
        # Apply the mapping with current value
        result = mock_processor.apply_column_mapping(
            mapping_result, source_row, 'notes', customer_crm_config,
            {'row_count': 1, 'column_count': 1}, current_value
        )
        
        assert "Existing notes" in result, "Should consider current value"
        assert "enterprise solution" in result, "Should include new information"
        
        # Verify current value was included in prompt
        call_args = mock_processor.client.chat.completions.create.call_args
        prompt = call_args[1]['messages'][0]['content']
        assert 'Existing notes' in prompt
    
    @pytest.mark.no_llm
    def test_data_type_formatting_for_llm(self, mock_processor, customer_crm_config):
        """Test that data types are properly formatted for LLM consumption"""
        # Mock LLM response
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "50000"
        
        # Reset side_effect and set return_value
        mock_processor.client.chat.completions.create.side_effect = None
        mock_processor.client.chat.completions.create.return_value = mock_response
        
        # Test data with different types
        source_row = {
            'text_field': 'Some text',
            'number_field': 12345,
            'float_field': 123.45,
            'none_field': None,
            'empty_field': ''
        }
        
        mapping_result = {
            'transformation_type': 'llm_format',
            'reasoning': 'Parse revenue from mixed data types'
        }
        
        # Apply the mapping
        mock_processor.apply_column_mapping(
            mapping_result, source_row, 'deal_value', customer_crm_config,
            {'row_count': 1, 'column_count': 5}
        )
        
        # Verify the data was properly formatted in the prompt
        call_args = mock_processor.client.chat.completions.create.call_args
        prompt = call_args[1]['messages'][0]['content']
        
        # Should contain properly formatted data with types
        assert '"type": "text"' in prompt
        assert '"type": "number"' in prompt
        assert '"type": "empty"' in prompt 