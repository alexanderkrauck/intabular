"""
Pytest configuration and fixtures for InTabular tests
"""

import os
import sys
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from intabular.main import setup_llm_client
from intabular.core.config import GatekeeperConfig
from intabular.core.analyzer import DataframeAnalyzer
from intabular.core.strategy import DataframeIngestionStrategy
from intabular.core.processor import DataframeIngestionProcessor


@pytest.fixture(scope="session")
def test_data_dir():
    """Get the test data directory path"""
    return Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def openai_client():
    """Setup OpenAI client if API key is available"""
    if not os.getenv('OPENAI_API_KEY'):
        pytest.skip("OPENAI_API_KEY not set, skipping LLM tests")
    return setup_llm_client()


@pytest.fixture
def mock_openai_client():
    """Mock OpenAI client for tests that don't need real API calls"""
    mock_client = Mock()
    
    # Mock a typical strategy response
    mock_strategy_response = Mock()
    mock_strategy_response.choices = [Mock()]
    mock_strategy_response.choices[0].message.content = '''
    {
        "reasoning": "Direct email mapping with normalization",
        "transformation_type": "format",
        "transformation_rule": "email_address.lower().strip()"
    }
    '''
    
    # Mock a typical analysis response
    mock_analysis_response = Mock()
    mock_analysis_response.choices = [Mock()]
    mock_analysis_response.choices[0].message.content = '''
    {
        "has_header": true,
        "semantic_purpose": "Contact management data",
        "reasoning": "Contains contact information fields"
    }
    '''
    
    # Mock column analysis response
    mock_column_response = Mock()
    mock_column_response.choices = [Mock()]
    mock_column_response.choices[0].message.content = '''
    {
        "reasoning": "Email field contains email addresses",
        "data_type": "identifier",
        "purpose": "Primary contact method for customers"
    }
    '''
    
    mock_client.chat.completions.create.side_effect = [
        mock_analysis_response,
        mock_column_response,
        mock_strategy_response
    ]
    
    return mock_client


@pytest.fixture
def customer_crm_config(test_data_dir):
    """Load customer CRM configuration"""
    config_path = test_data_dir / "configs" / "customer_crm.yaml"
    return GatekeeperConfig.from_yaml(str(config_path))


@pytest.fixture
def simple_contacts_config(test_data_dir):
    """Load simple contacts configuration"""
    config_path = test_data_dir / "configs" / "simple_contacts.yaml"
    return GatekeeperConfig.from_yaml(str(config_path))


@pytest.fixture
def minimal_schema_config(test_data_dir):
    """Load minimal schema configuration"""
    config_path = test_data_dir / "configs" / "minimal_schema.yaml"
    return GatekeeperConfig.from_yaml(str(config_path))


@pytest.fixture
def industrial_monitoring_config(test_data_dir):
    """Load industrial monitoring configuration"""
    config_path = test_data_dir / "configs" / "industrial_monitoring.yaml"
    return GatekeeperConfig.from_yaml(str(config_path))


@pytest.fixture
def perfect_match_df(test_data_dir):
    """Load perfect match CSV data"""
    csv_path = test_data_dir / "csv" / "perfect_match.csv"
    return pd.read_csv(csv_path)


@pytest.fixture
def format_transform_df(test_data_dir):
    """Load format transformation CSV data"""
    csv_path = test_data_dir / "csv" / "format_transform.csv"
    return pd.read_csv(csv_path)


@pytest.fixture
def llm_complex_df(test_data_dir):
    """Load complex LLM parsing CSV data"""
    csv_path = test_data_dir / "csv" / "llm_complex.csv"
    return pd.read_csv(csv_path)


@pytest.fixture
def edge_cases_df(test_data_dir):
    """Load edge cases CSV data"""
    csv_path = test_data_dir / "csv" / "edge_cases.csv"
    return pd.read_csv(csv_path)


@pytest.fixture
def industrial_sensors_df(test_data_dir):
    """Load industrial sensor measurement data"""
    csv_path = test_data_dir / "csv" / "industrial_sensors.csv"
    return pd.read_csv(csv_path)


@pytest.fixture
def financial_transactions_df(test_data_dir):
    """Load financial transaction data"""
    csv_path = test_data_dir / "csv" / "financial_transactions.csv"
    return pd.read_csv(csv_path)


@pytest.fixture
def lab_results_df(test_data_dir):
    """Load laboratory results data"""
    csv_path = test_data_dir / "csv" / "lab_results.csv"
    return pd.read_csv(csv_path)


@pytest.fixture
def empty_target_df(customer_crm_config):
    """Create empty target DataFrame with correct columns"""
    columns = list(customer_crm_config.all_columns.keys())
    return pd.DataFrame(columns=columns)


@pytest.fixture
def empty_industrial_target_df(industrial_monitoring_config):
    """Create empty target DataFrame for industrial data"""
    columns = list(industrial_monitoring_config.all_columns.keys())
    return pd.DataFrame(columns=columns)


@pytest.fixture
def analyzer(openai_client, customer_crm_config):
    """Create DataframeAnalyzer instance"""
    return DataframeAnalyzer(openai_client, customer_crm_config)


@pytest.fixture
def industrial_analyzer(openai_client, industrial_monitoring_config):
    """Create DataframeAnalyzer instance for industrial data"""
    return DataframeAnalyzer(openai_client, industrial_monitoring_config)


@pytest.fixture
def mock_analyzer(mock_openai_client, customer_crm_config):
    """Create DataframeAnalyzer instance with mock client"""
    return DataframeAnalyzer(mock_openai_client, customer_crm_config)


@pytest.fixture
def strategy_creator(openai_client):
    """Create DataframeIngestionStrategy instance"""
    return DataframeIngestionStrategy(openai_client)


@pytest.fixture
def mock_strategy_creator(mock_openai_client):
    """Create DataframeIngestionStrategy instance with mock client"""
    return DataframeIngestionStrategy(mock_openai_client)


@pytest.fixture
def processor(openai_client):
    """Create DataframeIngestionProcessor instance"""
    return DataframeIngestionProcessor(openai_client)


@pytest.fixture
def mock_processor(mock_openai_client):
    """Create DataframeIngestionProcessor instance with mock client"""
    return DataframeIngestionProcessor(mock_openai_client)


@pytest.fixture(autouse=True)
def cleanup_output_files(test_data_dir):
    """Clean up output files after each test"""
    yield
    output_dir = test_data_dir / "output"
    if output_dir.exists():
        for file in output_dir.glob("*.csv"):
            try:
                file.unlink()
            except OSError:
                pass  # File might be in use, ignore


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests based on patterns and requirements"""
    for item in items:
        # Mark tests that use real LLM fixtures
        llm_fixtures = ['openai_client', 'analyzer', 'strategy_creator', 'processor', 'industrial_analyzer']
        if any(fixture in item.fixturenames for fixture in llm_fixtures):
            item.add_marker(pytest.mark.llm)
        
        # Mark tests that use mock fixtures (no LLM required)
        mock_fixtures = ['mock_analyzer', 'mock_strategy_creator', 'mock_processor']
        if any(fixture in item.fixturenames for fixture in mock_fixtures):
            item.add_marker(pytest.mark.no_llm)
        
        # Mark tests by data type based on fixture names
        if any('industrial' in fixture for fixture in item.fixturenames):
            item.add_marker(pytest.mark.industrial)
        if any('financial' in fixture for fixture in item.fixturenames):
            item.add_marker(pytest.mark.financial)
        if any('lab_results' in fixture for fixture in item.fixturenames):
            item.add_marker(pytest.mark.scientific)
        if any('customer' in fixture or 'crm' in fixture for fixture in item.fixturenames):
            item.add_marker(pytest.mark.customer)
        
        # Mark integration tests
        if 'test_integration' in item.nodeid:
            item.add_marker(pytest.mark.integration)
        elif any(word in item.nodeid.lower() for word in ['test_format', 'test_perfect', 'test_edge']):
            item.add_marker(pytest.mark.unit) 