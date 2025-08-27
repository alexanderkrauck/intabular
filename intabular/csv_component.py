"""
CSV and file handling component for InTabular.
Separates file I/O concerns from core business logic.
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict
from openai import OpenAI
import os

from intabular.core.config import GatekeeperConfig
from intabular.core.logging_config import get_logger
from intabular.core.analyzer import DataframeAnalyzer
from intabular.core.strategy import DataframeIngestionStrategy
from intabular.core.processor import DataframeIngestionProcessor


def setup_llm_client() -> OpenAI:
    """Initialize LLM client - copied from main to avoid circular import"""
    logger = get_logger('csv_component')
    
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.critical("OPENAI_API_KEY environment variable not set")
        raise ValueError("OPENAI_API_KEY environment variable is required")
    
    base_url = os.getenv('INTABULAR_BASE_URL')
    organization = os.getenv('INTABULAR_ORGANIZATION')
    
    client_kwargs = {'api_key': api_key}
    if base_url:
        client_kwargs['base_url'] = base_url
    if organization:
        client_kwargs['organization'] = organization
    
    client = OpenAI(**client_kwargs)
    
    # Test connection
    try:
        client.models.list()
        logger.info("LLM client initialized")
    except Exception as e:
        logger.error(f"LLM API connection failed: {e}")
        raise
    
    return client


def add_prose_to_table(yaml_config_file: str, prose_description: str) -> pd.DataFrame:
    """
    Add data to an existing table using a prose description.
    
    Args:
        yaml_config_file: Path to YAML configuration
        prose_description: Natural language description of the data to add
        
    Returns:
        pd.DataFrame: The updated table with the new row
    """
    logger = get_logger('csv_component')
    
    logger.info(f"Adding prose to table: {yaml_config_file}")
    logger.info(f"Description: {prose_description}")
    
    # Load configuration
    schema = GatekeeperConfig.from_yaml(yaml_config_file)
    
    # Load or create target DataFrame
    if Path(schema.target_file_path).exists():
        df_target = pd.read_csv(schema.target_file_path)
        logger.info(f"Loaded existing target: {len(df_target)} rows")
    else:
        df_target = pd.DataFrame(columns=schema.get_enrichment_column_names())
        logger.info(f"Created empty target with {len(schema.get_enrichment_column_names())} columns")
    
    # Convert prose to structured data
    logger.info("Converting prose description to structured data...")
    structured_data = _prose_to_structured_data(prose_description, schema)
    
    # Create a single-row DataFrame from the structured data
    df_prose = pd.DataFrame([structured_data])
    logger.info(f"Structured data: {structured_data}")
    
    # Initialize components
    client = setup_llm_client()
    analyzer = DataframeAnalyzer(client, schema)
    strategy_creator = DataframeIngestionStrategy(client)
    processor = DataframeIngestionProcessor(client)
    
    # Analyze the prose-generated DataFrame
    logger.info("Analyzing structured data...")
    df_analysis = analyzer.analyze_dataframe_structure(df_prose)
    
    # Create intelligent strategy for the single row
    logger.info("Creating field-mapping strategy...")
    strategy = strategy_creator.create_ingestion_strategy(schema, df_analysis)
    
    # Execute ingestion
    logger.info("Executing ingestion...")
    result = processor.execute_ingestion(
        df_prose,
        df_target,
        strategy,
        schema,
        df_analysis.general_ingestion_analysis
    )
    
    # Save results
    result.to_csv(schema.target_file_path, index=False)
    logger.info(f"Saved {len(result)} rows to {schema.target_file_path}")
    
    return result


def _prose_to_structured_data(prose_description: str, schema: GatekeeperConfig) -> Dict[str, str]:
    """
    Convert a prose description to structured data matching the schema.
    
    Args:
        prose_description: Natural language description
        schema: Target schema configuration
        
    Returns:
        Dict[str, str]: Structured data matching schema columns
    """
    logger = get_logger('csv_component')
    
    # Initialize LLM client
    client = setup_llm_client()
    
    # Get schema information
    column_descriptions = {}
    for col_name, col_info in schema.enrichment_columns.items():
        if isinstance(col_info, dict) and 'description' in col_info:
            column_descriptions[col_name] = col_info['description']
        else:
            column_descriptions[col_name] = f"Column: {col_name}"
    
    # Create prompt for LLM
    import textwrap
    prompt = textwrap.dedent(f"""
        Convert the following prose description into structured data that matches the target schema.
        
        TARGET SCHEMA PURPOSE: {schema.purpose}
        
        TARGET COLUMNS AND DESCRIPTIONS:
        {chr(10).join([f"- {col}: {desc}" for col, desc in column_descriptions.items()])}
        
        PROSE DESCRIPTION TO CONVERT:
        {prose_description}
        
        INSTRUCTIONS:
        1. Extract information from the prose description that matches each target column
        2. If information is not available for a column, leave it empty
        3. Format the data appropriately for each column type
        4. Return ONLY a JSON object with column names as keys and extracted values as strings
        5. Do not include any explanations or additional text
        
        Example format:
        {{"column1": "value1", "column2": "value2", "column3": ""}}
    """).strip()
    
    try:
        response = client.chat.completions.create(
            model=os.getenv('INTABULAR_PROCESSOR_MODEL', 'gpt-4o-mini'),
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=1000
        )
        
        result_text = response.choices[0].message.content.strip()
        logger.debug(f"LLM response: {result_text}")
        
        # Parse JSON response
        import json
        structured_data = json.loads(result_text)
        
        # Ensure all schema columns are present
        final_data = {}
        for col_name in schema.get_enrichment_column_names():
            final_data[col_name] = structured_data.get(col_name, "")
        
        logger.info(f"Converted prose to structured data with {len(final_data)} fields")
        return final_data
        
    except Exception as e:
        logger.error(f"Failed to convert prose to structured data: {e}")
        # Fallback: create empty structured data
        return {col_name: "" for col_name in schema.get_enrichment_column_names()}


def run_csv_ingestion_pipeline(yaml_config_file: str, csv_to_ingest: str) -> pd.DataFrame:
    """
    CSV wrapper around core ingestion logic.
    Loads CSV files, runs Mode 3 ingestion, saves result.
    
    Args:
        yaml_config_file: Path to YAML configuration
        csv_to_ingest: Path to CSV file to ingest
        
    Returns:
        pd.DataFrame: The processed result
    """
    logger = get_logger('csv_component')
    
    logger.info(f"CSV ingestion: {csv_to_ingest} -> {yaml_config_file}")
    
    # Load configuration and CSV files
    schema = GatekeeperConfig.from_yaml(yaml_config_file)
    df_ingest = pd.read_csv(csv_to_ingest)
    
    # Load or create target DataFrame
    if Path(schema.target_file_path).exists():
        df_target = pd.read_csv(schema.target_file_path)
        logger.info(f"Loaded existing target: {len(df_target)} rows")
    else:
        df_target = pd.DataFrame(columns=schema.get_enrichment_column_names())
        logger.info(f"Created empty target with {len(schema.get_enrichment_column_names())} columns")
    
    # Core Mode 3 logic (explicit schema ingestion) - implemented directly here
    logger.info(f"Mode 3: Explicit schema ingestion - {len(df_ingest)} + {len(df_target)} rows")
    
    # Initialize components
    client = setup_llm_client()
    analyzer = DataframeAnalyzer(client, schema)
    strategy_creator = DataframeIngestionStrategy(client)
    processor = DataframeIngestionProcessor(client)
    
    logger.info(f"Schema: {schema.purpose[:80]}... ({len(schema.get_enrichment_column_names())} columns)")
    
    # Analyze the ingestion DataFrame
    logger.info("Analyzing ingestion DataFrame...")
    df_analysis = analyzer.analyze_dataframe_structure(df_ingest)
    
    # Create intelligent strategy
    logger.info("Creating field-mapping strategy...")
    strategy = strategy_creator.create_ingestion_strategy(schema, df_analysis)
    
    # Execute ingestion
    logger.info("Executing ingestion...")
    result = processor.execute_ingestion(
        df_ingest,
        df_target,
        strategy,
        schema,
        df_analysis.general_ingestion_analysis
    )
    
    # Save results
    result.to_csv(schema.target_file_path, index=False)
    logger.info(f"Saved {len(result)} rows to {schema.target_file_path}")
    
    return result


def create_config_from_csv(table_path: str, purpose: str, output_yaml: Optional[str] = None) -> str:
    """
    Create YAML configuration by analyzing existing CSV structure.
    
    Args:
        table_path: Path to existing CSV table
        purpose: Business purpose description
        output_yaml: Optional output YAML file path
        
    Returns:
        str: Path to created YAML file
    """
    logger = get_logger('csv_component')
    
    if Path(table_path).exists():
        logger.info(f"Analyzing CSV structure: {table_path}")
        df = pd.read_csv(table_path)
        enrichment_columns = {col: {"description": f"Auto-detected column: {col}", "match_type": "semantic"} 
                             for col in df.columns}
        logger.info(f"Found {len(enrichment_columns)} columns")
    else:
        # Default enrichment columns
        default_cols = ["email", "first_name", "last_name", "company", "title", "phone", "website"]
        enrichment_columns = {col: {"description": f"Default column: {col}", "match_type": "semantic"} 
                             for col in default_cols}
        logger.warning(f"CSV not found, using default columns: {list(enrichment_columns.keys())}")
    
    config = GatekeeperConfig(
        purpose=purpose,
        enrichment_columns=enrichment_columns,
        target_file_path=table_path
    )
    
    yaml_file = output_yaml or f"{Path(table_path).stem}_config.yaml"
    config.to_yaml(yaml_file)
    
    logger.info(f"Configuration saved: {yaml_file}")
    return yaml_file


# Public API functions for programmatic access
def add_data_from_prose(table_df: pd.DataFrame, schema_config: GatekeeperConfig, 
                       description: str) -> pd.DataFrame:
    """
    Add data to a DataFrame using a prose description.
    
    This is the programmatic API for adding prose-based data to tables.
    
    Args:
        table_df: Existing DataFrame to add data to
        schema_config: Schema configuration defining the table structure
        description: Natural language description of the data to add
        
    Returns:
        pd.DataFrame: Updated DataFrame with the new row added
        
    Examples:
        >>> import pandas as pd
        >>> from intabular.core.config import GatekeeperConfig
        >>> from intabular.csv_component import add_data_from_prose
        >>> 
        >>> # Create a simple customer table
        >>> df = pd.DataFrame({
        ...     'email': ['john@example.com'],
        ...     'name': ['John Doe'],
        ...     'company': ['Example Corp']
        ... })
        >>> 
        >>> # Define schema
        >>> config = GatekeeperConfig(
        ...     purpose="Customer data",
        ...     enrichment_columns={
        ...         'email': {'description': 'Email address', 'is_entity_identifier': True},
        ...         'name': {'description': 'Full name', 'is_entity_identifier': True},  
        ...         'company': {'description': 'Company name', 'is_entity_identifier': False}
        ...     },
        ...     target_file_path="customers.csv"
        ... )
        >>> 
        >>> # Add new customer using prose
        >>> description = "Jane Smith from Tech Inc, email jane@tech.com"
        >>> updated_df = add_data_from_prose(df, config, description)
    """
    logger = get_logger('csv_component')
    
    logger.info(f"Adding prose data to DataFrame with {len(table_df)} existing rows")
    logger.info(f"Description: {description}")
    
    # Convert prose to structured data
    logger.info("Converting prose description to structured data...")
    structured_data = _prose_to_structured_data(description, schema_config)
    
    # Create a single-row DataFrame from the structured data
    df_prose = pd.DataFrame([structured_data])
    logger.info(f"Structured data: {structured_data}")
    
    # Initialize components
    client = setup_llm_client()
    analyzer = DataframeAnalyzer(client, schema_config)
    strategy_creator = DataframeIngestionStrategy(client)
    processor = DataframeIngestionProcessor(client)
    
    # Analyze the prose-generated DataFrame
    logger.info("Analyzing structured data...")
    df_analysis = analyzer.analyze_dataframe_structure(df_prose)
    
    # Create intelligent strategy for the single row
    logger.info("Creating field-mapping strategy...")
    strategy = strategy_creator.create_ingestion_strategy(schema_config, df_analysis)
    
    # Execute ingestion
    logger.info("Executing ingestion...")
    result = processor.execute_ingestion(
        df_prose,
        table_df,
        strategy,
        schema_config,
        df_analysis.general_ingestion_analysis
    )
    
    logger.info(f"Successfully added 1 row. DataFrame now has {len(result)} rows")
    return result 