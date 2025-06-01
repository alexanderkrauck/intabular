"""
Tests for format transformation scenarios where source columns need deterministic transformation
"""

import pytest
import pandas as pd


class TestFormatTransformations:
    """Test cases for format-based transformations"""
    
    def test_email_normalization(self, processor):
        """Test email address normalization"""
        test_cases = [
            ('JOHN.DOE@ACME.COM', 'email_address.lower().strip()', 'john.doe@acme.com'),
            ('  jane@example.com  ', 'email_address.strip().lower()', 'jane@example.com'),
            ('Bob.Wilson@STARTUP.IO', 'email_address.lower()', 'bob.wilson@startup.io')
        ]
        
        for input_email, rule, expected in test_cases:
            source_data = {'email_address': input_email}
            result = processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to normalize {input_email}"
    
    def test_name_combination(self, processor):
        """Test combining first and last names"""
        test_cases = [
            ({'first_name': 'John', 'last_name': 'Doe'}, 
             "f'{first_name.lower()} {last_name.lower()}'", 
             'john doe'),
            ({'first_name': '  Jane  ', 'last_name': '  Smith  '}, 
             "f'{first_name.strip().lower()} {last_name.strip().lower()}'", 
             'jane smith'),
            ({'first_name': 'Bob', 'last_name': 'Wilson'}, 
             "f'{first_name} {last_name}'.lower()", 
             'bob wilson')
        ]
        
        for source_data, rule, expected in test_cases:
            result = processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to combine names: {source_data}"
    
    def test_phone_normalization(self, processor):
        """Test phone number normalization"""
        test_cases = [
            ('(555) 123-4567', "re.sub(r'[^\\d]', '', phone_number)", '5551234567'),
            ('555.987.6543', "re.sub(r'[^\\d]', '', phone_number)", '5559876543'),
            ('555-555-0123', "re.sub(r'[^\\d]', '', phone_number)", '5555550123')
        ]
        
        for input_phone, rule, expected in test_cases:
            source_data = {'phone_number': input_phone}
            result = processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to normalize phone: {input_phone}"
    
    def test_value_extraction(self, processor):
        """Test extracting numeric values from text"""
        test_cases = [
            ('$50,000', "re.sub(r'[^\\d]', '', value)", '50000'),
            ('$75K', "value.replace('$', '').replace('K', '000').replace(',', '')", '75000'),
            ('25000', "value", '25000')
        ]
        
        for input_value, rule, expected in test_cases:
            source_data = {'value': input_value}
            result = processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to extract value: {input_value}"
    
    def test_location_normalization(self, processor):
        """Test location formatting"""
        test_cases = [
            ('San Francisco CA', "city_state.lower().replace(' ca', ', ca, usa')", 'san francisco, ca, usa'),
            ('New York NY', "city_state.lower().replace(' ny', ', ny, usa')", 'new york, ny, usa'),
            ('Austin Texas', "city_state.lower().replace(' texas', ', tx, usa')", 'austin, tx, usa')
        ]
        
        for input_location, rule, expected in test_cases:
            source_data = {'city_state': input_location}
            result = processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to normalize location: {input_location}"
    
    def test_transformation_rule_validation(self, processor):
        """Test transformation rule validation"""
        # Valid rules should pass
        valid_rules = [
            'email.lower()',
            "f'{first_name} {last_name}'",
            "re.sub(r'[^\\d]', '', phone)",
            'str(value).strip()'
        ]
        
        for rule in valid_rules:
            assert processor.validate_transformation_rule(rule), f"Valid rule failed: {rule}"
        
        # Invalid rules should fail
        invalid_rules = [
            'import os',
            'exec("print(hello)")',
            'eval("1+1")',
            'open("file.txt")',
            '__import__("os")'
        ]
        
        for rule in invalid_rules:
            assert not processor.validate_transformation_rule(rule), f"Invalid rule passed: {rule}"
    
    def test_error_handling_in_transformations(self, processor):
        """Test error handling when transformations fail"""
        # This should raise an error due to missing variable
        with pytest.raises(ValueError):
            processor.execute_transformation('missing_variable.lower()', {'existing': 'value'})
        
        # This should raise an error due to invalid syntax
        with pytest.raises(ValueError):
            processor.execute_transformation('invalid syntax here', {'test': 'value'})
    
    def test_format_transformation_strategy(self, analyzer, strategy_creator, customer_crm_config, format_transform_df):
        """Test that format transformation data creates format strategies"""
        # Analyze the format transformation dataframe
        analysis = analyzer.analyze_dataframe_structure(format_transform_df, "Format transformation test data")
        
        # Create strategy
        strategy = strategy_creator.create_ingestion_strategy(customer_crm_config, analysis)
        
        # Check that we get some format transformations
        entity_mappings = strategy.no_merge_column_mappings
        format_mappings = [mapping for mapping in entity_mappings.values() 
                          if mapping['transformation_type'] == 'format']
        
        assert len(format_mappings) > 0, "Should have at least some format transformations"
        
        # Each format mapping should have a transformation rule
        for mapping in format_mappings:
            assert 'transformation_rule' in mapping, "Format mappings must have transformation rules"
            assert mapping['transformation_rule'], "Transformation rule cannot be empty"
    
    def test_current_value_merging(self, processor):
        """Test merging with current values"""
        source_data = {'notes': 'New information'}
        current_value = 'Existing notes'
        
        # Test rule that combines current and new values
        rule = "f'{current} | {notes}'"
        result = processor.execute_transformation(rule, source_data, current_value)
        
        assert result == 'Existing notes | New information', "Should combine current and new values"
    
    def test_complex_format_combinations(self, processor):
        """Test complex format rule combinations"""
        source_data = {
            'first_name': 'John',
            'last_name': 'Doe', 
            'email_addr': 'JOHN.DOE@ACME.COM',
            'company': 'ACME Corporation'
        }
        
        # Complex rule combining multiple fields
        rule = "f'{first_name.lower()} {last_name.lower()} ({email_addr.lower()}) - {company.title()}'"
        result = processor.execute_transformation(rule, source_data)
        
        expected = 'john doe (john.doe@acme.com) - Acme Corporation'
        assert result == expected, "Complex format combinations should work"
    
    def test_safe_namespace_restrictions(self, processor):
        """Test that only safe functions are available in transformation namespace"""
        # These should work (safe functions)
        safe_tests = [
            ('str(123)', {'test': 'value'}, '123'),
            ('len("hello")', {'test': 'value'}, '5'),
            ('min(1, 2, 3)', {'test': 'value'}, '1'),
            ('max(1, 2, 3)', {'test': 'value'}, '3')
        ]
        
        for rule, data, expected in safe_tests:
            result = processor.execute_transformation(rule, data)
            assert str(result) == expected, f"Safe function test failed: {rule}" 