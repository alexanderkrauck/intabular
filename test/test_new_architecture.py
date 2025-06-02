"""
Tests for the DataFrame-first architecture and CSV component.
Tests actual working functionality following TDD principles.
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch
import tempfile
import os
import yaml


class TestDataFrameArchitecture:
    """Test the DataFrame-first core architecture - only implemented functionality"""
    
    @pytest.mark.unit
    def test_mode2_requires_api_key_when_no_mock(self, customer_crm_config):
        """Test Mode 2: Transform DataFrame to schema requires API key for real functionality"""
        from intabular.main import ingest_to_schema
        
        # Create test DataFrame
        df_ingest = pd.DataFrame({
            'email_address': ['test@example.com', 'jane@company.com'],
            'full_name': ['John Doe', 'Jane Smith'],
            'company': ['Test Corp', 'Company Inc']
        })
        
        # Should fail without API key
        if not os.getenv('OPENAI_API_KEY'):
            with pytest.raises(ValueError, match="OPENAI_API_KEY"):
                ingest_to_schema(df_ingest, customer_crm_config)
        else:
            # If API key is available, this should work
            result = ingest_to_schema(df_ingest, customer_crm_config)
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
    
    @pytest.mark.unit
    def test_mode3_explicit_schema_requires_api_key(self, customer_crm_config):
        """Test Mode 3: Explicit schema ingestion requires API key"""
        from intabular.main import ingest_with_explicit_schema
        
        df_ingest = pd.DataFrame({
            'email': ['test@example.com'],
            'full_name': ['john doe'],
            'company_name': ['test corp']
        })
        df_target = pd.DataFrame({
            'email': ['existing@example.com'],
            'full_name': ['jane smith'],
            'company_name': ['existing corp'],
            'phone': ['+1-555-0101'],
            'location': ['san francisco, ca, usa'],
            'deal_stage': ['prospect'],
            'deal_value': ['$10k'],
            'notes': ['existing customer']
        })
        
        # Should fail without API key
        if not os.getenv('OPENAI_API_KEY'):
            with pytest.raises(ValueError, match="OPENAI_API_KEY"):
                ingest_with_explicit_schema(df_ingest, df_target, customer_crm_config)
        else:
            # If API key is available, this should work
            result = ingest_with_explicit_schema(df_ingest, df_target, customer_crm_config)
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0

    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_setup_llm_client_requires_api_key(self):
        """Test that setup_llm_client requires API key"""
        from intabular.main import setup_llm_client
        
        # Temporarily unset API key
        original_key = os.getenv('OPENAI_API_KEY')
        try:
            if 'OPENAI_API_KEY' in os.environ:
                del os.environ['OPENAI_API_KEY']
            
            with pytest.raises(ValueError, match="OPENAI_API_KEY"):
                setup_llm_client()
        finally:
            # Restore original key if it existed
            if original_key is not None:
                os.environ['OPENAI_API_KEY'] = original_key


class TestCSVComponent:
    """Test the CSV wrapper component"""
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_create_config_from_csv_existing(self):
        """Test creating config from existing CSV"""
        from intabular.csv_component import create_config_from_csv
        
        # Create temporary CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('email,first_name,last_name\n')
            f.write('john@test.com,John,Doe\n')
            csv_path = f.name
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                yaml_path = str(Path(temp_dir) / "test_config.yaml")
                result_path = create_config_from_csv(csv_path, "Test purpose", yaml_path)
                
                assert result_path == yaml_path
                assert Path(yaml_path).exists()
                
                # Verify YAML content
                from intabular.core.config import GatekeeperConfig
                config = GatekeeperConfig.from_yaml(yaml_path)
                assert config.purpose == "Test purpose"
                assert 'email' in config.enrichment_columns
                assert 'first_name' in config.enrichment_columns
                assert 'last_name' in config.enrichment_columns
        finally:
            os.unlink(csv_path)
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_create_config_from_csv_nonexistent(self):
        """Test creating config from non-existent CSV (uses defaults)"""
        from intabular.csv_component import create_config_from_csv
        
        with tempfile.TemporaryDirectory() as temp_dir:
            nonexistent_csv = str(Path(temp_dir) / "nonexistent.csv")
            yaml_path = str(Path(temp_dir) / "test_config.yaml")
            
            result_path = create_config_from_csv(nonexistent_csv, "Test purpose", yaml_path)
            
            assert result_path == yaml_path
            assert Path(yaml_path).exists()
            
            # Verify default columns
            from intabular.core.config import GatekeeperConfig
            config = GatekeeperConfig.from_yaml(yaml_path)
            assert config.purpose == "Test purpose"
            default_cols = ["email", "first_name", "last_name", "company", "title", "phone", "website"]
            for col in default_cols:
                assert col in config.enrichment_columns
    
    @pytest.mark.unit
    def test_csv_ingestion_pipeline_requires_api_key(self):
        """Test CSV ingestion pipeline requires API key"""
        from intabular.csv_component import run_csv_ingestion_pipeline
        
        # Create temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as csv_f:
            csv_f.write('email,name\njohn@test.com,John Doe\n')
            csv_path = csv_f.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as yaml_f:
            yaml_f.write('''
purpose: "Test configuration"
enrichment_columns:
  email:
    description: "Email address"
    match_type: "semantic"
    is_entity_identifier: true
    identity_indication: 1.0
  full_name:
    description: "Full name"
    match_type: "semantic"
    is_entity_identifier: false
target_file_path: "test_target.csv"
sample_rows: 3
''')
            yaml_path = yaml_f.name
        
        try:
            # Should fail without API key
            if not os.getenv('OPENAI_API_KEY'):
                with pytest.raises(ValueError, match="OPENAI_API_KEY"):
                    run_csv_ingestion_pipeline(yaml_path, csv_path)
            else:
                # If API key is available, this should work
                result = run_csv_ingestion_pipeline(yaml_path, csv_path)
                assert isinstance(result, pd.DataFrame)
        finally:
            os.unlink(csv_path)
            os.unlink(yaml_path)


class TestCSVFileOperations:
    """Test CSV file input/output operations"""
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_csv_reading_various_formats(self):
        """Test reading CSV files with various formats"""
        test_cases = [
            # Standard CSV
            ('email,name,company\njohn@test.com,John Doe,Test Corp\n', 3, 1),
            # CSV with quotes
            ('email,name,company\n"john@test.com","John, Jr","Test Corp"\n', 3, 1),
            # CSV with empty values
            ('email,name,company\njohn@test.com,,Test Corp\n,Jane Doe,\n', 3, 2),
            # CSV with unicode
            ('email,name,company\njohñ@tëst.com,Jöhn Döe,Tést Cörp\n', 3, 1),
        ]
        
        for csv_content, expected_cols, expected_rows in test_cases:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
                f.write(csv_content)
                csv_path = f.name
            
            try:
                df = pd.read_csv(csv_path)
                assert len(df.columns) == expected_cols
                assert len(df) == expected_rows
            finally:
                os.unlink(csv_path)
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_csv_writing_preservation(self):
        """Test that CSV writing preserves data correctly"""
        # Create test data with various edge cases
        test_df = pd.DataFrame({
            'email': ['john@test.com', 'jane@company.com', 'bob@example.org'],
            'name': ['John Doe', 'Jane, Smith', 'Bob "Bobby" Wilson'],
            'score': [1.5, 2.0, None],
            'active': [True, False, True],
            'notes': ['Normal text', 'Text with\nnewline', '']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            csv_path = f.name
        
        try:
            # Write and read back
            test_df.to_csv(csv_path, index=False)
            loaded_df = pd.read_csv(csv_path)
            
            # Verify structure
            assert list(loaded_df.columns) == list(test_df.columns)
            assert len(loaded_df) == len(test_df)
            
            # Verify specific data preservation
            assert loaded_df['email'].tolist() == test_df['email'].tolist()
            assert loaded_df['name'].tolist() == test_df['name'].tolist()
            
        finally:
            os.unlink(csv_path)
    
    @pytest.mark.no_llm  
    @pytest.mark.unit
    def test_csv_error_handling(self):
        """Test CSV error handling"""
        # Test reading non-existent file
        with pytest.raises(FileNotFoundError):
            pd.read_csv("nonexistent_file.csv")
        
        # Test reading malformed CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('incomplete,csv\nrow1,col1,extra_column\n')  # Mismatched columns
            csv_path = f.name
        
        try:
            # pandas usually handles this gracefully, but let's verify it doesn't crash
            df = pd.read_csv(csv_path)
            assert isinstance(df, pd.DataFrame)
        finally:
            os.unlink(csv_path)


class TestYAMLConfigOperations:
    """Test YAML configuration handling"""
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_yaml_config_creation_and_loading(self):
        """Test creating and loading YAML configurations"""
        from intabular.core.config import GatekeeperConfig
        
        # Create a config
        config_data = {
            'purpose': 'Test customer database',
            'enrichment_columns': {
                'email': {
                    'description': 'Customer email address',
                    'match_type': 'semantic'
                },
                'full_name': {
                    'description': 'Customer full name',
                    'match_type': 'semantic'
                }
            },
            'target_file_path': 'customers.csv',
            'sample_rows': 5
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            yaml_path = f.name
        
        try:
            # Load the config
            config = GatekeeperConfig.from_yaml(yaml_path)
            
            # Verify all fields
            assert config.purpose == 'Test customer database'
            assert 'email' in config.enrichment_columns
            assert 'full_name' in config.enrichment_columns
            assert config.target_file_path == 'customers.csv'
            assert config.sample_rows == 5
            
            # Verify column details
            assert config.get_column_description('email') == 'Customer email address'
            assert config.get_column_match_type('email') == 'semantic'
            
        finally:
            os.unlink(yaml_path)
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_yaml_config_roundtrip(self):
        """Test that YAML configs can be saved and loaded without data loss"""
        from intabular.core.config import GatekeeperConfig
        
        # Create original config
        original_config = GatekeeperConfig(
            purpose="Round-trip test database",
            enrichment_columns={
                'email': {'description': 'Email field', 'match_type': 'semantic'},
                'user_id': {'description': 'User identifier', 'match_type': 'exact'},
                'name': {'description': 'User name', 'match_type': 'semantic'}
            },
            target_file_path="test.csv",
            sample_rows=10
        )
        
        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = str(Path(temp_dir) / "roundtrip_config.yaml")
            
            # Save config
            original_config.to_yaml(yaml_path)
            assert Path(yaml_path).exists()
            
            # Load config
            loaded_config = GatekeeperConfig.from_yaml(yaml_path)
            
            # Verify all data is preserved
            assert loaded_config.purpose == original_config.purpose
            assert loaded_config.target_file_path == original_config.target_file_path
            assert loaded_config.sample_rows == original_config.sample_rows
            assert loaded_config.get_enrichment_column_names() == original_config.get_enrichment_column_names()
            
            # Verify column details
            for col in original_config.get_enrichment_column_names():
                assert loaded_config.get_column_description(col) == original_config.get_column_description(col)
                assert loaded_config.get_column_match_type(col) == original_config.get_column_match_type(col)
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_yaml_config_validation(self):
        """Test YAML config validation and error handling"""
        from intabular.core.config import GatekeeperConfig
        
        # Test invalid YAML
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write('invalid: yaml: content: [unclosed bracket')
            invalid_yaml_path = f.name
        
        try:
            with pytest.raises(yaml.YAMLError):
                GatekeeperConfig.from_yaml(invalid_yaml_path)
        finally:
            os.unlink(invalid_yaml_path)
        
        # Test missing required fields
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({'purpose': 'Missing enrichment_columns'}, f)
            incomplete_yaml_path = f.name
        
        try:
            with pytest.raises(KeyError):
                GatekeeperConfig.from_yaml(incomplete_yaml_path)
        finally:
            os.unlink(incomplete_yaml_path)
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_yaml_config_special_characters(self):
        """Test YAML configs with special characters and unicode"""
        from intabular.core.config import GatekeeperConfig
        
        config_with_special_chars = GatekeeperConfig(
            purpose="Datenbänk für Küstömer Mänägémént",  # Unicode
            enrichment_columns={
                'emäil': {'description': 'E-mail address with spëcial chars', 'match_type': 'semantic'},
                'näme': {'description': 'Name with "quotes" and symbols @#$%', 'match_type': 'semantic'}
            }
        )
        
        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = str(Path(temp_dir) / "special_chars.yaml")
            
            # Save and load
            config_with_special_chars.to_yaml(yaml_path)
            loaded_config = GatekeeperConfig.from_yaml(yaml_path)
            
            # Verify unicode preservation
            assert loaded_config.purpose == config_with_special_chars.purpose
            assert 'emäil' in loaded_config.enrichment_columns
            assert 'näme' in loaded_config.enrichment_columns
            assert loaded_config.get_column_description('emäil') == config_with_special_chars.get_column_description('emäil')


class TestArchitecturalIntegration:
    """Test integration between DataFrame API and CSV component"""
    
    @pytest.mark.unit
    def test_dataframe_and_csv_consistency(self, customer_crm_config):
        """Test that DataFrame and CSV APIs work together - basic import consistency"""
        from intabular.main import ingest_to_schema
        from intabular.csv_component import create_config_from_csv
        
        # Test that imports work and functions are callable
        assert callable(ingest_to_schema)
        assert callable(create_config_from_csv)
        
        # Test basic parameter validation - should fail with API key requirement
        df_test = pd.DataFrame({'email': ['test@example.com'], 'name': ['Test User']})
        
        if not os.getenv('OPENAI_API_KEY'):
            with pytest.raises(ValueError, match="OPENAI_API_KEY"):
                ingest_to_schema(df_test, customer_crm_config)
        else:
            # If API key is available, the function should be callable
            result_df = ingest_to_schema(df_test, customer_crm_config)
            assert isinstance(result_df, pd.DataFrame)
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_import_consistency(self):
        """Test that imports work consistently across the architecture"""
        # Test that all core functions can be imported
        from intabular.main import (
            ingest_with_implicit_schema,
            ingest_to_schema,
            ingest_with_explicit_schema,
            infer_schema_from_target,
            setup_llm_client
        )
        
        from intabular.csv_component import (
            run_csv_ingestion_pipeline,
            create_config_from_csv
        )
        
        from intabular.cli import main as cli_main
        
        # All imports should succeed
        assert callable(ingest_with_implicit_schema)
        assert callable(ingest_to_schema)
        assert callable(ingest_with_explicit_schema)
        assert callable(infer_schema_from_target)
        assert callable(setup_llm_client)
        assert callable(run_csv_ingestion_pipeline)
        assert callable(create_config_from_csv)
        assert callable(cli_main)


class TestEndToEndFileWorkflows:
    """Test complete file-based workflows"""
    
    @pytest.mark.no_llm
    @pytest.mark.unit
    def test_complete_csv_to_yaml_workflow(self):
        """Test creating YAML config from CSV and verifying the complete flow"""
        from intabular.csv_component import create_config_from_csv
        from intabular.core.config import GatekeeperConfig
        
        # Create source CSV
        test_data = """email,first_name,last_name,company,phone
john@acme.com,John,Doe,Acme Corp,555-0101
jane@techco.com,Jane,Smith,TechCo Inc,555-0202
bob@startup.io,Bob,Wilson,Startup LLC,555-0303"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as csv_f:
            csv_f.write(test_data)
            csv_path = csv_f.name
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                yaml_path = str(Path(temp_dir) / "generated_config.yaml")
                
                # Generate config from CSV
                result_path = create_config_from_csv(csv_path, "Customer management system", yaml_path)
                assert Path(result_path).exists()
                
                # Load and verify the generated config
                config = GatekeeperConfig.from_yaml(result_path)
                assert config.purpose == "Customer management system"
                
                # Verify all CSV columns are in the config
                csv_df = pd.read_csv(csv_path)
                for col in csv_df.columns:
                    assert col in config.enrichment_columns
                    assert config.get_column_description(col).startswith("Auto-detected column:")
                    assert config.get_column_match_type(col) == "semantic"
                
                # Verify target file path is set correctly
                assert config.target_file_path == csv_path
                
        finally:
            os.unlink(csv_path)
    
    @pytest.mark.unit
    def test_csv_pipeline_workflow_requires_api_key(self):
        """Test CSV pipeline workflow requires API key"""
        from intabular.csv_component import run_csv_ingestion_pipeline
        
        # Create test CSV data
        source_csv_data = """email,name,company
newuser@example.com,New User,Example Corp
another@company.com,Another User,Company Inc"""
        
        # Create test YAML config
        yaml_config = """
purpose: "Test customer database"
enrichment_columns:
  email:
    description: "Customer email address"
    match_type: "semantic"
    is_entity_identifier: true
    identity_indication: 1.0
  full_name:
    description: "Customer full name"  
    match_type: "semantic"
    is_entity_identifier: false
  company_name:
    description: "Company name"
    match_type: "semantic"
    is_entity_identifier: false
target_file_path: "target.csv"
sample_rows: 3
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as csv_f:
            csv_f.write(source_csv_data)
            csv_path = csv_f.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as yaml_f:
            yaml_f.write(yaml_config)
            yaml_path = yaml_f.name
        
        try:
            # Should fail without API key
            if not os.getenv('OPENAI_API_KEY'):
                with pytest.raises(ValueError, match="OPENAI_API_KEY"):
                    run_csv_ingestion_pipeline(yaml_path, csv_path)
            else:
                # If API key is available, test that it works
                result = run_csv_ingestion_pipeline(yaml_path, csv_path)
                assert isinstance(result, pd.DataFrame)
                assert len(result) > 0
                
        finally:
            os.unlink(csv_path)
            os.unlink(yaml_path) 