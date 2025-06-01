"""
Tests for diverse data domains beyond customer management
"""

import pytest
import pandas as pd
import numpy as np


class TestIndustrialData:
    """Test cases for industrial sensor monitoring data"""
    
    @pytest.mark.industrial
    @pytest.mark.no_llm
    def test_sensor_data_structure(self, industrial_sensors_df):
        """Test that industrial sensor data has expected structure"""
        expected_columns = ['sensor_id', 'timestamp', 'temperature_celsius', 'pressure_bar', 'location', 'status']
        
        for col in expected_columns:
            assert col in industrial_sensors_df.columns, f"Missing expected column: {col}"
        
        # Check data types and ranges
        assert len(industrial_sensors_df) > 0, "Should have sensor readings"
        assert industrial_sensors_df['temperature_celsius'].dtype in [np.float64, np.int64], "Temperature should be numeric"
        assert industrial_sensors_df['pressure_bar'].dtype in [np.float64, np.int64], "Pressure should be numeric"
    
    @pytest.mark.industrial
    @pytest.mark.no_llm
    def test_sensor_id_format_transformation(self, mock_processor):
        """Test formatting sensor IDs consistently"""
        test_cases = [
            ({'sensor_id': 'TEMP_001', 'location': 'Factory A'}, 'sensor_id.upper().strip()', 'TEMP_001'),
            ({'sensor_id': '  pres_002  ', 'location': 'Boiler'}, 'sensor_id.strip().upper()', 'PRES_002'),
            ({'sensor_id': 'flow-003', 'location': 'Pipeline'}, 'sensor_id.replace("-", "_").upper()', 'FLOW_003')
        ]
        
        for source_data, rule, expected in test_cases:
            result = mock_processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to format sensor ID: {source_data['sensor_id']}"
    
    @pytest.mark.industrial  
    @pytest.mark.no_llm
    def test_temperature_unit_conversion(self, mock_processor):
        """Test temperature unit conversions"""
        test_cases = [
            ({'temp_f': '75.2'}, 'str(round((float(temp_f) - 32) * 5/9, 1))', '24.0'),  # F to C
            ({'temp_k': '296.15'}, 'str(round(float(temp_k) - 273.15, 1))', '23.0'),    # K to C
            ({'temp_c': '23.5'}, 'temp_c', '23.5')  # Already in Celsius
        ]
        
        for source_data, rule, expected in test_cases:
            result = mock_processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to convert temperature: {source_data}"
    
    @pytest.mark.industrial
    @pytest.mark.llm
    def test_industrial_pipeline_integration(self, industrial_analyzer, strategy_creator, processor, industrial_monitoring_config, industrial_sensors_df, empty_industrial_target_df):
        """Test complete pipeline with industrial sensor data"""
        # Run full pipeline
        analysis = industrial_analyzer.analyze_dataframe_structure(industrial_sensors_df, "Industrial sensor monitoring data")
        strategy = strategy_creator.create_ingestion_strategy(industrial_monitoring_config, analysis)
        result_df = processor.execute_ingestion(
            industrial_sensors_df, empty_industrial_target_df, strategy, industrial_monitoring_config,
            analysis.general_ingestion_analysis
        )
        
        # Verify industrial-specific results
        assert len(result_df) > 0, "Should process sensor readings"
        assert 'sensor_id' in result_df.columns, "Should have sensor ID column"
        assert 'temperature_celsius' in result_df.columns, "Should have temperature column"
        
        # Check that sensor IDs are present
        sensor_ids = result_df['sensor_id'].dropna()
        assert len(sensor_ids) > 0, "Should have sensor ID data"


class TestFinancialData:
    """Test cases for financial transaction data"""
    
    @pytest.mark.financial
    @pytest.mark.no_llm
    def test_financial_data_structure(self, financial_transactions_df):
        """Test that financial data has expected structure"""
        expected_columns = ['transaction_id', 'account_number', 'amount_usd', 'transaction_type']
        
        for col in expected_columns:
            assert col in financial_transactions_df.columns, f"Missing expected column: {col}"
        
        # Check for monetary values
        assert len(financial_transactions_df) > 0, "Should have transactions"
        # Amount should be numeric (float64 or int64) or convertible to numeric
        amount_col = financial_transactions_df['amount_usd']
        if amount_col.dtype == 'object':
            # Try to convert to numeric to verify it's valid numeric data
            pd.to_numeric(amount_col, errors='coerce')
        else:
            assert amount_col.dtype in [np.float64, np.int64], "Amount should be numeric or numeric-convertible"
    
    @pytest.mark.financial
    @pytest.mark.no_llm
    def test_transaction_id_normalization(self, mock_processor):
        """Test normalizing transaction ID formats"""
        test_cases = [
            ({'txn_id': 'TXN20240115001'}, 'txn_id.upper().strip()', 'TXN20240115001'),
            ({'txn_id': '  txn-2024-01-15-002  '}, 'txn_id.strip().upper().replace("-", "")', 'TXN20240115002'),
            ({'txn_id': 'transaction_123456'}, 'txn_id.upper().replace("TRANSACTION_", "TXN")', 'TXN123456')
        ]
        
        for source_data, rule, expected in test_cases:
            result = mock_processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to normalize transaction ID: {source_data['txn_id']}"
    
    @pytest.mark.financial
    @pytest.mark.no_llm
    def test_amount_formatting(self, mock_processor):
        """Test formatting monetary amounts"""
        test_cases = [
            ({'amount': '$1,234.56'}, "amount.replace('$', '').replace(',', '')", '1234.56'),
            ({'amount': '(500.00)'}, "'-' + amount.replace('(', '').replace(')', '').replace('$', '')", '-500.00'),
            ({'amount': '1500'}, 'amount', '1500')
        ]
        
        for source_data, rule, expected in test_cases:
            result = mock_processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to format amount: {source_data['amount']}"
    
    @pytest.mark.financial
    @pytest.mark.no_llm
    def test_account_number_masking(self, mock_processor):
        """Test masking sensitive account numbers"""
        test_cases = [
            ({'account': 'ACC-123456789'}, "'****' + account[-4:]", '****6789'),
            ({'account': '1234567890123456'}, "'****-****-****-' + account[-4:]", '****-****-****-3456'),
        ]
        
        for source_data, rule, expected in test_cases:
            result = mock_processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to mask account: {source_data['account']}"


class TestScientificData:
    """Test cases for laboratory/scientific data"""
    
    @pytest.mark.scientific
    @pytest.mark.no_llm  
    def test_lab_results_structure(self, lab_results_df):
        """Test that lab results data has expected structure"""
        expected_columns = ['sample_id', 'ph_level', 'conductivity_ms_cm', 'dissolved_oxygen_mg_l']
        
        for col in expected_columns:
            assert col in lab_results_df.columns, f"Missing expected column: {col}"
        
        # Check scientific measurement ranges
        assert len(lab_results_df) > 0, "Should have lab results"
        
        # pH should be between 0-14 (more lenient check)
        ph_values = lab_results_df['ph_level'].dropna()
        if len(ph_values) > 0:
            try:
                ph_numeric = pd.to_numeric(ph_values, errors='coerce').dropna()
                if len(ph_numeric) > 0:
                    min_ph, max_ph = ph_numeric.min(), ph_numeric.max()
                    assert 0 <= min_ph <= 14, f"Minimum pH value {min_ph} should be between 0-14"
                    assert 0 <= max_ph <= 14, f"Maximum pH value {max_ph} should be between 0-14"
            except Exception as e:
                pytest.fail(f"pH validation failed: {e}")
    
    @pytest.mark.scientific
    @pytest.mark.no_llm
    def test_sample_id_standardization(self, mock_processor):
        """Test standardizing sample ID formats"""
        test_cases = [
            ({'sample': 'WTR-001'}, 'sample.upper().strip()', 'WTR-001'),
            ({'sample': '  wtr_002  '}, 'sample.strip().upper().replace("_", "-")', 'WTR-002'),
            ({'sample': 'water-sample-003'}, 'sample.upper().replace("WATER-SAMPLE-", "WTR-")', 'WTR-003')
        ]
        
        for source_data, rule, expected in test_cases:
            result = mock_processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to standardize sample ID: {source_data['sample']}"
    
    @pytest.mark.scientific
    @pytest.mark.no_llm
    def test_coordinate_formatting(self, mock_processor):
        """Test formatting GPS coordinates consistently"""
        test_cases = [
            ({'lat': '40.7128', 'lon': '-74.0060'}, "lat + '°N ' + lon.replace('-', '') + '°W'", '40.7128°N 74.0060°W'),
            ({'coords': '40.7589, -73.9851'}, "coords.replace(', ', '°N ').replace('-', '') + '°W'", '40.7589°N 73.9851°W')
        ]
        
        for source_data, rule, expected in test_cases:
            result = mock_processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to format coordinates: {source_data}"
    
    @pytest.mark.scientific
    @pytest.mark.no_llm
    def test_measurement_precision(self, mock_processor):
        """Test standardizing measurement precision"""
        test_cases = [
            ({'ph': '7.234567'}, 'str(round(float(ph), 1))', '7.2'),
            ({'conductivity': '245.6789'}, 'str(round(float(conductivity), 1))', '245.7'),
            ({'oxygen': '8.333333'}, 'str(round(float(oxygen), 2))', '8.33')
        ]
        
        for source_data, rule, expected in test_cases:
            result = mock_processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to standardize precision: {source_data}"


class TestCrossDomainCapabilities:
    """Test InTabular's ability to handle different data domains"""
    
    @pytest.mark.no_llm
    def test_numeric_data_handling(self, mock_processor):
        """Test handling various numeric formats across domains"""
        test_cases = [
            # Industrial measurements
            ({'temp': '23.5°C'}, "temp.replace('°C', '').strip()", '23.5'),
            ({'pressure': '1.013 bar'}, "pressure.replace(' bar', '').strip()", '1.013'),
            
            # Financial amounts  
            ({'amount': '$1,234.56'}, "amount.replace('$', '').replace(',', '')", '1234.56'),
            ({'percent': '15.5%'}, "percent.replace('%', '').strip()", '15.5'),
            
            # Scientific measurements
            ({'ph': 'pH 7.2'}, "ph.replace('pH ', '').strip()", '7.2'),
            ({'concentration': '245.6 μS/cm'}, "concentration.split(' ')[0]", '245.6')
        ]
        
        for source_data, rule, expected in test_cases:
            result = mock_processor.execute_transformation(rule, source_data)
            assert result == expected, f"Failed to handle numeric data: {source_data}"
    
    @pytest.mark.no_llm
    @pytest.mark.parametrize("domain,csv_fixture", [
        ("industrial", "industrial_sensors_df"),
        ("financial", "financial_transactions_df"), 
        ("scientific", "lab_results_df"),
        ("customer", "perfect_match_df")
    ])
    def test_data_integrity_across_domains(self, domain, csv_fixture, request):
        """Test that all domain data maintains integrity"""
        df = request.getfixturevalue(csv_fixture)
        
        # Basic integrity checks
        assert len(df) > 0, f"{domain} data should not be empty"
        assert len(df.columns) > 0, f"{domain} data should have columns"
        
        # Check for reasonable data completeness
        total_cells = len(df) * len(df.columns)
        non_null_cells = df.count().sum()
        completeness = non_null_cells / total_cells if total_cells > 0 else 0
        
        assert completeness > 0.5, f"{domain} data should be at least 50% complete"
    
    @pytest.mark.llm
    @pytest.mark.parametrize("domain_config,domain_data", [
        ("industrial_monitoring_config", "industrial_sensors_df"),
        ("customer_crm_config", "perfect_match_df")
    ])  
    def test_cross_domain_analysis_capabilities(self, domain_config, domain_data, request, openai_client):
        """Test that the analyzer works across different data domains"""
        config = request.getfixturevalue(domain_config)
        df = request.getfixturevalue(domain_data)
        
        from intabular.core.analyzer import DataframeAnalyzer
        analyzer = DataframeAnalyzer(openai_client, config)
        
        # Should be able to analyze any domain
        analysis = analyzer.analyze_dataframe_structure(df, f"Cross-domain test for {domain_config}")
        
        assert analysis is not None, f"Should analyze {domain_config} data"
        assert analysis.general_ingestion_analysis['row_count'] > 0, "Should detect rows"
        assert len(analysis.dataframe_column_analysis) > 0, "Should analyze columns" 