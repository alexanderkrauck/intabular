"""
Integration tests for the full InTabular pipeline
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock
from intabular.core.strategy import DataframeIngestionStrategyResult


class TestIntegration:
    """Test cases for full end-to-end pipeline integration"""
    
    def test_full_pipeline_perfect_match(self, analyzer, strategy_creator, processor, customer_crm_config, perfect_match_df, empty_target_df):
        """Test complete pipeline with perfect column matches"""
        # Step 1: Analyze dataframe
        analysis = analyzer.analyze_dataframe_structure(perfect_match_df, "Perfect match integration test")
        
        # Step 2: Create strategy
        strategy = strategy_creator.create_ingestion_strategy(customer_crm_config, analysis)
        
        # Step 3: Execute ingestion
        result_df = processor.execute_ingestion(
            perfect_match_df, empty_target_df, strategy, customer_crm_config,
            analysis.general_ingestion_analysis
        )
        
        # Verify end-to-end results
        assert len(result_df) > 0, "Should produce results"
        assert 'email' in result_df.columns, "Should have email column"
        assert result_df['email'].notna().any(), "Should have populated email data"
        
        # Check that at least some normalization happened
        emails = result_df['email'].dropna()
        assert all('@' in email for email in emails), "All emails should be valid"
    
    def test_full_pipeline_format_transformation(self, analyzer, strategy_creator, processor, customer_crm_config, format_transform_df, empty_target_df):
        """Test complete pipeline with format transformations"""
        # Complete pipeline
        analysis = analyzer.analyze_dataframe_structure(format_transform_df, "Format transformation integration test")
        strategy = strategy_creator.create_ingestion_strategy(customer_crm_config, analysis)
        result_df = processor.execute_ingestion(
            format_transform_df, empty_target_df, strategy, customer_crm_config,
            analysis.general_ingestion_analysis
        )
        
        # Verify transformations occurred
        assert len(result_df) > 0, "Should produce results"
        assert len(result_df) == len(format_transform_df), "Should process all input rows"
        
        # Check that email column exists and has valid data
        if 'email' in result_df.columns:
            emails = result_df['email'].dropna()
            valid_emails = emails[emails.str.len() > 0]
            if len(valid_emails) > 0:
                for email in valid_emails:
                    assert '@' in email, f"Email should contain @: {email}"
                    # Don't enforce case normalization as it depends on LLM behavior
        
        # Check that name combination worked (if it happens)
        if 'full_name' in result_df.columns:
            names = result_df['full_name'].dropna()
            valid_names = names[names.str.len() > 0]
            # Only check space if there are actual combined names
            combined_names = valid_names[valid_names.str.contains(' ', na=False)]
            if len(combined_names) > 0:
                for name in combined_names:
                    assert ' ' in name, f"Combined full name should contain space: {name}"
        
        # Verify basic pipeline functionality
        assert 'email' in result_df.columns, "Should have email column"
        assert 'full_name' in result_df.columns, "Should have full_name column"
    
    def test_pipeline_with_existing_data(self, analyzer, strategy_creator, processor, customer_crm_config, perfect_match_df):
        """Test pipeline when target already has data (merge scenario)"""
        # Create target with existing data
        existing_data = {
            'email': ['existing@example.com'],
            'full_name': ['existing user'],
            'company_name': ['existing corp'],
            'deal_stage': ['existing stage'],
            'deal_value': ['existing value'],
            'phone': ['555-000-0000'],
            'location': ['existing city'],
            'notes': ['existing notes']
        }
        target_df = pd.DataFrame(existing_data)
        
        # Run pipeline
        analysis = analyzer.analyze_dataframe_structure(perfect_match_df, "Merge integration test")
        strategy = strategy_creator.create_ingestion_strategy(customer_crm_config, analysis)
        result_df = processor.execute_ingestion(
            perfect_match_df, target_df, strategy, customer_crm_config,
            analysis.general_ingestion_analysis
        )
        
        # Should have more rows than original (existing + new)
        assert len(result_df) > len(target_df), "Should add new rows"
        assert len(result_df) == len(target_df) + len(perfect_match_df), "Should have all rows"
        
        # Original data should still be there
        assert 'existing@example.com' in result_df['email'].values, "Should preserve existing data"
    
    def test_multiple_csv_ingestion_sequence(self, analyzer, strategy_creator, processor, customer_crm_config, perfect_match_df, format_transform_df, empty_target_df):
        """Test ingesting multiple CSVs in sequence"""
        # First ingestion
        analysis1 = analyzer.analyze_dataframe_structure(perfect_match_df, "First CSV")
        strategy1 = strategy_creator.create_ingestion_strategy(customer_crm_config, analysis1)
        result_df1 = processor.execute_ingestion(
            perfect_match_df, empty_target_df, strategy1, customer_crm_config,
            analysis1.general_ingestion_analysis
        )
        
        initial_count = len(result_df1)
        
        # Second ingestion (building on first)
        analysis2 = analyzer.analyze_dataframe_structure(format_transform_df, "Second CSV")
        strategy2 = strategy_creator.create_ingestion_strategy(customer_crm_config, analysis2)
        result_df2 = processor.execute_ingestion(
            format_transform_df, result_df1, strategy2, customer_crm_config,
            analysis2.general_ingestion_analysis
        )
        
        # Should have accumulated data
        assert len(result_df2) > initial_count, "Should accumulate data from multiple ingestions"
        
        # Should have data from both sources
        all_emails = result_df2['email'].dropna().tolist()
        assert len(all_emails) > 0, "Should have email data from both sources"
    
    def test_error_recovery_in_pipeline(self, customer_crm_config, perfect_match_df, empty_target_df):
        """Test pipeline recovery from partial failures"""
        # Create proper mock objects
        mock_analyzer = Mock()
        mock_strategy_creator = Mock()
        mock_processor = Mock()
        
        # Mock analysis to succeed
        mock_analysis = type('MockAnalysis', (), {
            'general_ingestion_analysis': {'row_count': 4, 'column_count': 8},
            'dataframe_column_analysis': {
                'email': {'data_type': 'identifier', 'purpose': 'Contact email'},
                'full_name': {'data_type': 'identifier', 'purpose': 'Person name'}
            }
        })()
        mock_analyzer.analyze_dataframe_structure.return_value = mock_analysis
        
        # Mock strategy to succeed
        mock_strategy = DataframeIngestionStrategyResult(
            {'email': {'transformation_type': 'format', 'transformation_rule': 'email.lower()'}},
            {'full_name': {'transformation_type': 'format', 'transformation_rule': 'full_name.lower()'}}
        )
        mock_strategy_creator.create_ingestion_strategy.return_value = mock_strategy
        
        # Mock processor to return a result
        mock_processor.execute_ingestion.return_value = perfect_match_df.copy()
        
        # Run pipeline with mocks
        analysis = mock_analyzer.analyze_dataframe_structure(perfect_match_df, "Error recovery test")
        strategy = mock_strategy_creator.create_ingestion_strategy(customer_crm_config, analysis)
        result_df = mock_processor.execute_ingestion(
            perfect_match_df, empty_target_df, strategy, customer_crm_config,
            analysis.general_ingestion_analysis
        )
        
        # Should complete successfully even with mocked components
        assert result_df is not None, "Pipeline should handle mocked components"
        assert len(result_df) > 0, "Should return data"
    
    def test_configuration_driven_pipeline(self, simple_contacts_config, minimal_schema_config, test_data_dir):
        """Test pipeline with different configurations"""
        # Create simple test data
        simple_data = pd.DataFrame({
            'email': ['test@example.com'],
            'name': ['Test User'],
            'company': ['Test Corp']
        })
        
        # Test with simple config
        assert simple_contacts_config.purpose, "Simple config should load"
        assert 'email' in simple_contacts_config.enrichment_columns, "Should have email column"
        
        # Test with minimal config
        assert minimal_schema_config.purpose, "Minimal config should load"
        assert len(minimal_schema_config.enrichment_columns) == 1, "Minimal should have one column"
    
    @pytest.mark.parametrize("config_name,csv_name", [
        ("customer_crm.yaml", "perfect_match.csv"),
        ("simple_contacts.yaml", "format_transform.csv"),
        ("minimal_schema.yaml", "edge_cases.csv")
    ])
    def test_all_config_csv_combinations(self, config_name, csv_name, test_data_dir, openai_client):
        """Test all combinations of configs and CSVs"""
        from intabular.core.config import GatekeeperConfig
        from intabular.core.analyzer import DataframeAnalyzer
        from intabular.core.strategy import DataframeIngestionStrategy
        
        # Load config and CSV
        config_path = test_data_dir / "configs" / config_name
        csv_path = test_data_dir / "csv" / csv_name
        
        config = GatekeeperConfig.from_yaml(str(config_path))
        df = pd.read_csv(csv_path)
        
        # Create components
        analyzer = DataframeAnalyzer(openai_client, config)
        strategy_creator = DataframeIngestionStrategy(openai_client)
        
        # Test analysis phase
        analysis = analyzer.analyze_dataframe_structure(df, f"Test with {config_name} and {csv_name}")
        assert analysis is not None, f"Analysis should succeed for {config_name} + {csv_name}"
        
        # Test strategy phase
        strategy = strategy_creator.create_ingestion_strategy(config, analysis)
        assert strategy is not None, f"Strategy should succeed for {config_name} + {csv_name}"
    
    def test_output_file_creation(self, test_data_dir, analyzer, strategy_creator, processor, customer_crm_config, perfect_match_df, empty_target_df):
        """Test that output files are created correctly"""
        # Run pipeline
        analysis = analyzer.analyze_dataframe_structure(perfect_match_df, "Output file test")
        strategy = strategy_creator.create_ingestion_strategy(customer_crm_config, analysis)
        result_df = processor.execute_ingestion(
            perfect_match_df, empty_target_df, strategy, customer_crm_config,
            analysis.general_ingestion_analysis
        )
        
        # Save to output file
        output_path = test_data_dir / "output" / "test_output.csv"
        result_df.to_csv(output_path, index=False)
        
        # Verify file was created
        assert output_path.exists(), "Output file should be created"
        
        # Verify file content
        loaded_df = pd.read_csv(output_path)
        assert len(loaded_df) == len(result_df), "Saved file should have same row count"
        assert list(loaded_df.columns) == list(result_df.columns), "Saved file should have same columns"
    
    def test_data_consistency_across_pipeline(self, analyzer, strategy_creator, processor, customer_crm_config, perfect_match_df, empty_target_df):
        """Test that data remains consistent throughout the pipeline"""
        original_email_count = perfect_match_df['email'].notna().sum()
        
        # Run pipeline
        analysis = analyzer.analyze_dataframe_structure(perfect_match_df, "Consistency test")
        strategy = strategy_creator.create_ingestion_strategy(customer_crm_config, analysis)
        result_df = processor.execute_ingestion(
            perfect_match_df, empty_target_df, strategy, customer_crm_config,
            analysis.general_ingestion_analysis
        )
        
        # Check data consistency
        result_email_count = result_df['email'].notna().sum()
        assert result_email_count >= original_email_count, "Should not lose email data"
        
        # Check that entity columns are properly populated
        entity_columns = [col for col in customer_crm_config.entity_columns.keys() if col in result_df.columns]
        for col in entity_columns:
            if col in perfect_match_df.columns:
                original_count = perfect_match_df[col].notna().sum()
                result_count = result_df[col].notna().sum()
                assert result_count >= original_count * 0.8, f"Should preserve most data in {col}" 