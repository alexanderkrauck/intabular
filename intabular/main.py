"""
Main module for InTabular - Intelligent Table Data Ingestion.
"""

import os
import sys
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from openai import OpenAI
from dotenv import load_dotenv

from .core.config import TableConfig
from .core.analyzer import CSVAnalyzer
from .core.strategy import IngestionStrategy
from .core.processor import DataProcessor

# Load environment variables
load_dotenv()


class AdaptiveMerger:
    """Main class for intelligent CSV data ingestion"""
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the adaptive merger with OpenAI API key"""
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        
        if not self.api_key:
            raise ValueError("❌ OpenAI API key is required! Please set OPENAI_API_KEY in .env file")
        
        try:
            self.client = OpenAI(api_key=self.api_key)
            print("✅ Using OpenAI LLM for intelligent data ingestion")
            
            # Test the API connection
            test_response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": "Test connection"}],
                max_tokens=5
            )
            print(f"🔗 API connection verified")
            
        except Exception as e:
            raise RuntimeError(f"❌ Failed to initialize OpenAI client: {e}")
        
        # Initialize components
        self.analyzer = CSVAnalyzer(self.client)
        self.strategy_creator = IngestionStrategy(self.client)
        self.processor = DataProcessor()
    
    def analyze_unknown_csv(self, csv_path: str) -> Dict[str, Any]:
        """Analyze unknown CSV structure and infer semantic meaning of columns"""
        return self.analyzer.analyze_unknown_csv(csv_path)
    
    def create_ingestion_strategy(self, target_config: TableConfig, unknown_analysis: Dict) -> Dict[str, Any]:
        """Create strategy to ingest unknown CSV into target table structure"""
        return self.strategy_creator.create_ingestion_strategy(target_config, unknown_analysis)
    
    def execute_ingestion(self, target_table: str, unknown_csv: str, 
                         strategy: Dict[str, Any], target_config: TableConfig) -> pd.DataFrame:
        """Execute field-by-field ingestion based on strategy"""
        return self.processor.execute_ingestion(target_table, unknown_csv, strategy, target_config)
    
    def ingest_csv(self, yaml_config_file: str, csv_to_ingest: str) -> pd.DataFrame:
        """
        Complete pipeline to intelligently ingest unknown CSV into target table structure
        
        Args:
            yaml_config_file: Path to YAML configuration file defining target schema and output location
            csv_to_ingest: Path to unknown CSV file to be ingested
            
        Returns:
            DataFrame with ingested data mapped to target schema
        """
        
        print("🚀 Starting intelligent CSV ingestion pipeline")
        print(f"📋 Config: {yaml_config_file}")
        print(f"📄 Input CSV: {csv_to_ingest}")
        
        # Load target configuration from YAML
        if not Path(yaml_config_file).exists():
            raise FileNotFoundError(f"❌ YAML configuration file not found: {yaml_config_file}")
        
        print(f"📝 Loading target schema configuration...")
        target_config = TableConfig.from_yaml(yaml_config_file)
        print(f"   🎯 Purpose: {target_config.purpose[:80]}...")
        print(f"   📊 Target columns: {len(target_config.get_enrichment_column_names())}")
        print(f"   📜 Policy: {target_config.column_policy}")
        
        # Get output file from YAML config
        output_file = target_config.target
        if not output_file:
            output_file = "ingested_data.csv"
            print(f"   ⚠️  No target specified in YAML, using default: {output_file}")
        else:
            print(f"   📂 Target file: {output_file}")
        
        # Determine target table file (check if exists or create empty)
        target_columns = target_config.get_enrichment_column_names()
        target_table_file = f"target_table_temp.csv"
        
        # Create empty target table structure
        empty_target = pd.DataFrame(columns=target_columns)
        empty_target.to_csv(target_table_file, index=False)
        print(f"📋 Created target table structure with {len(target_columns)} columns")
        
        # Analyze unknown CSV
        print(f"🔍 Analyzing unknown CSV structure...")
        unknown_analysis = self.analyze_unknown_csv(csv_to_ingest)
        
        print(f"✅ CSV Analysis Complete:")
        print(f"   📊 Purpose: {unknown_analysis.get('table_purpose', 'Unknown')}")
        print(f"   📦 Source: {unknown_analysis.get('data_source', 'Unknown')}")
        print(f"   📋 Input columns: {unknown_analysis.get('column_count', 0)}")
        print(f"   📈 Input rows: {unknown_analysis.get('row_count', 0)}")
        
        # Create ingestion strategy
        print(f"🧠 Creating intelligent field-mapping strategy...")
        strategy = self.create_ingestion_strategy(target_config, unknown_analysis)
        
        strategy_confidence = strategy.get('confidence_score', 0)
        field_count = len(strategy.get('field_strategies', {}))
        unmapped_count = len(strategy.get('unmapped_source_columns', {}))
        
        print(f"✅ Strategy Created:")
        print(f"   🎯 Overall confidence: {strategy_confidence:.2f}")
        print(f"   🗺️  Target fields mapped: {field_count}")
        print(f"   ⚠️  Source columns unmapped: {unmapped_count}")
        
        # Execute ingestion
        print(f"🔀 Executing intelligent field-by-field ingestion...")
        ingested_df = self.execute_ingestion(target_table_file, csv_to_ingest, strategy, target_config)
        
        # Clean up temporary file
        if Path(target_table_file).exists():
            os.remove(target_table_file)
        
        # Save result to target specified in YAML
        print(f"💾 Saving results to: {output_file}")
        ingested_df.to_csv(output_file, index=False)
        
        # Display summary
        print(f"\n🎉 Ingestion Pipeline Complete!")
        print(f"   📄 Input: {csv_to_ingest} ({unknown_analysis.get('row_count', 0)} rows)")
        print(f"   📊 Output: {output_file} ({len(ingested_df)} rows)")
        print(f"   🎯 Columns mapped: {field_count}/{len(target_columns)}")
        print(f"   ✨ Strategy confidence: {strategy_confidence:.2f}")
        
        return ingested_df
    
    def create_table_config(self, table_path: str, purpose: str,
                          enrichment_columns: Union[List[str], Dict[str, str]] = None,
                          column_policy: str = "balanced") -> TableConfig:
        """Create and save a table configuration YAML file"""
        
        # If no enrichment columns provided, try to infer from existing table
        if enrichment_columns is None:
            if Path(table_path).exists():
                print(f"📋 Inferring schema from existing table: {table_path}")
                df = pd.read_csv(table_path)
                enrichment_columns = list(df.columns)
                print(f"   📊 Found {len(enrichment_columns)} columns")
            else:
                enrichment_columns = ["email", "full_name", "company_name"]
                print(f"   ⚠️  Table not found, using default columns: {enrichment_columns}")
        
        # Create config
        config = TableConfig(
            purpose=purpose,
            enrichment_columns=enrichment_columns,
            column_policy=column_policy,
            target=table_path
        )
        
        # Save to YAML
        yaml_file = table_path.replace('.csv', '_config.yaml')
        config.to_yaml(yaml_file)
        
        print(f"✅ Configuration saved to: {yaml_file}")
        print(f"   🎯 Purpose: {purpose}")
        print(f"   📊 Columns: {len(config.get_enrichment_column_names())}")
        print(f"   📜 Policy: {column_policy}")
        
        return config


def main():
    """CLI entry point for InTabular"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m intabular <yaml_config> <csv_file>     # Ingest CSV")
        print("  python -m intabular config <table> <purpose>     # Create config")
        print("  python -m intabular analyze <csv_file>           # Analyze CSV")
        sys.exit(1)
    
    try:
        merger = AdaptiveMerger()
        
        if sys.argv[1] == "config":
            # Create configuration
            if len(sys.argv) < 4:
                print("Usage: python -m intabular config <table_path> <purpose>")
                sys.exit(1)
            
            table_path = sys.argv[2]
            purpose = sys.argv[3]
            merger.create_table_config(table_path, purpose)
            
        elif sys.argv[1] == "analyze":
            # Analyze CSV
            if len(sys.argv) < 3:
                print("Usage: python -m intabular analyze <csv_file>")
                sys.exit(1)
            
            csv_file = sys.argv[2]
            analysis = merger.analyze_unknown_csv(csv_file)
            print(f"\n📊 Analysis complete for: {csv_file}")
            print(f"Business Purpose: {analysis.get('table_purpose', 'Unknown')}")
            print(f"Data Source: {analysis.get('data_source', 'Unknown')}")
            
        else:
            # Main ingestion
            if len(sys.argv) < 3:
                print("Usage: python -m intabular <yaml_config> <csv_file>")
                sys.exit(1)
            
            yaml_config = sys.argv[1]
            csv_file = sys.argv[2]
            result = merger.ingest_csv(yaml_config, csv_file)
            
            print(f"\n🎉 Successfully ingested {len(result)} rows!")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 