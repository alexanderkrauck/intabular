"""
Intelligent CSV analysis for semantic understanding.
"""

import json
import time
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Tuple
from openai import OpenAI
from .logging_config import get_logger, log_prompt_response
from concurrent.futures import ThreadPoolExecutor


class CSVAnalyzer:
    """Analyzes unknown CSV files to understand their structure and purpose"""
    
    def __init__(self, openai_client: OpenAI):
        self.client = openai_client
        self.max_parallel_calls = 5  # Limit parallel API calls
        self.logger = get_logger('analyzer')
    
    def analyze_csv_structure(self, csv_path: str) -> Dict[str, Any]:
        """Comprehensive analysis of CSV structure and content"""
        
        self.logger.info(f"📊 Starting CSV analysis for: {Path(csv_path).name}")
        
        # Load the CSV
        df = pd.read_csv(csv_path)
        
        self.logger.info(f"📈 CSV dimensions: {len(df)} rows × {len(df.columns)} columns",
                        extra={
                            'file_path': csv_path,
                            'row_count': len(df),
                            'column_count': len(df.columns),
                            'columns': list(df.columns)
                        })
        
        self.logger.info(f"📋 Columns: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
        
        # Analyze individual columns in parallel
        self.logger.info(f"🧠 Analyzing {len(df.columns)} columns individually...")
        column_semantics = self._analyze_columns_parallel(df)
        
        # Analyze overall table structure
        self.logger.info("🧠 Analyzing overall table structure...")
        table_analysis = self._analyze_table_structure(df, column_semantics)
        
        # Combine results
        analysis = {
            "file_path": csv_path,
            "row_count": len(df),
            "column_count": len(df.columns),
            "column_names": list(df.columns),
            "column_semantics": column_semantics,
            **table_analysis
        }
        
        self._log_analysis_results(analysis)
        
        return analysis
    
    def _analyze_columns_parallel(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze individual columns in parallel for better performance"""
        
        column_results = {}
        
        # Process columns in batches
        columns = list(df.columns)
        batch_size = self.max_parallel_calls
        
        for i in range(0, len(columns), batch_size):
            batch = columns[i:i + batch_size]
            
            self.logger.debug(f"Processing column batch {i//batch_size + 1} with {len(batch)} columns",
                            extra={'batch_columns': batch})
            
            with ThreadPoolExecutor(max_workers=min(len(batch), self.max_parallel_calls)) as executor:
                futures = {
                    executor.submit(self._analyze_single_column, df[col], col): col
                    for col in batch
                }
                
                for future in futures:
                    col_name = futures[future]
                    try:
                        result = future.result(timeout=30)
                        column_results[col_name] = result
                        self.logger.info(f"✅ {col_name}: {result.get('semantic_type', 'unknown')}",
                                       extra={'column': col_name, 'analysis': result})
                    except Exception as e:
                        self.logger.error(f"❌ {col_name}: Analysis failed - {e}",
                                        extra={'column': col_name, 'error': str(e)})
                        column_results[col_name] = self._get_fallback_column_analysis(col_name)
        
        return column_results
    
    def _analyze_single_column(self, series: pd.Series, col_name: str) -> Dict[str, Any]:
        """Analyze a single column to understand its semantic meaning"""
        
        self.logger.debug(f"Analyzing column: {col_name}",
                         extra={'column': col_name, 'non_null_count': series.count()})
        
        # Get sample values (non-null, unique)
        sample_values = series.dropna().unique()[:10].tolist()
        
        # Basic statistics
        stats = {
            "total_count": len(series),
            "non_null_count": series.count(),
            "unique_count": series.nunique(),
            "completeness": series.count() / len(series) if len(series) > 0 else 0
        }
        
        # Response schema for column analysis
        response_schema = {
            "type": "object",
            "properties": {
                "semantic_type": {
                    "type": "string",
                    "enum": [
                        "email", "phone", "name", "company", "title", "address", 
                        "city", "state", "country", "postal_code", "website", 
                        "date", "currency", "percentage", "identifier", "category", 
                        "description", "notes", "score", "rating", "unknown"
                    ]
                },
                "data_format": {"type": "string"},
                "business_value": {
                    "type": "string", 
                    "enum": ["high", "medium", "low"]
                },
                "data_quality": {
                    "type": "string",
                    "enum": ["excellent", "good", "fair", "poor"]
                },
                "standardization_needs": {"type": "string"},
                "potential_enrichment": {"type": "string"}
            },
            "required": ["semantic_type", "data_format", "business_value", "data_quality", "standardization_needs", "potential_enrichment"],
            "additionalProperties": False
        }
        
        prompt = f"""
        Analyze this CSV column to understand its semantic meaning and business value:
        
        COLUMN NAME: {col_name}
        
        SAMPLE VALUES (first 10 unique, non-null):
        {sample_values}
        
        COLUMN STATISTICS:
        - Total entries: {stats['total_count']}
        - Non-null entries: {stats['non_null_count']}
        - Unique values: {stats['unique_count']}
        - Completeness: {stats['completeness']:.2%}
        
        Please analyze this column and determine:
        1. The semantic type (what kind of data this represents)
        2. The current data format/structure
        3. Business value for contact enrichment and lead management
        4. Data quality assessment
        5. Any standardization needs
        6. Potential for enrichment or enhancement
        
        Be specific about the semantic type - use the most precise category available.
        Consider how this data could be useful for business intelligence and contact management.
        """
        
        start_time = time.time()
        
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "column_analysis",
                    "schema": response_schema
                }
            },
            temperature=0.1
        )
        
        duration = time.time() - start_time
        response_content = response.choices[0].message.content
        
        # Log the prompt and response
        log_prompt_response(
            self.logger, prompt, response_content,
            model="gpt-4o-mini", duration=duration
        )
        
        result = json.loads(response_content)
        result.update(stats)  # Add statistics to result
        
        return result
    
    def _analyze_table_structure(self, df: pd.DataFrame, column_semantics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze overall table structure and purpose"""
        
        self.logger.debug("Analyzing overall table structure and business purpose")
        
        # Prepare column summary for analysis
        column_summary = []
        for col, analysis in column_semantics.items():
            column_summary.append({
                "name": col,
                "type": analysis.get('semantic_type', 'unknown'),
                "business_value": analysis.get('business_value', 'low'),
                "quality": analysis.get('data_quality', 'unknown'),
                "completeness": analysis.get('completeness', 0)
            })
        
        # Response schema for table analysis
        response_schema = {
            "type": "object",
            "properties": {
                "table_purpose": {"type": "string"},
                "data_source": {
                    "type": "string",
                    "enum": [
                        "CRM export", "email marketing list", "lead generation", 
                        "customer database", "contact list", "sales prospects",
                        "event attendees", "survey responses", "manual entry",
                        "web scraping", "social media", "third-party data",
                        "unknown"
                    ]
                },
                "primary_entity": {
                    "type": "string",
                    "enum": ["person", "company", "lead", "customer", "contact", "prospect", "unknown"]
                },
                "confidence": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1
                },
                "quality_assessment": {
                    "type": "object",
                    "properties": {
                        "overall_completeness": {"type": "number"},
                        "data_consistency": {"type": "number"},
                        "standardization_level": {
                            "type": "string",
                            "enum": ["high", "medium", "low"]
                        }
                    },
                    "required": ["overall_completeness", "data_consistency", "standardization_level"]
                },
                "enrichment_potential": {
                    "type": "object",
                    "properties": {
                        "contact_enhancement": {"type": "string"},
                        "business_intelligence": {"type": "string"},
                        "data_quality_improvement": {"type": "string"}
                    },
                    "required": ["contact_enhancement", "business_intelligence", "data_quality_improvement"]
                }
            },
            "required": ["table_purpose", "data_source", "primary_entity", "confidence", "quality_assessment", "enrichment_potential"],
            "additionalProperties": False
        }
        
        prompt = f"""
        Analyze this CSV table structure to understand its business purpose and data source:
        
        TABLE OVERVIEW:
        - Total rows: {len(df)}
        - Total columns: {len(df.columns)}
        
        COLUMN ANALYSIS SUMMARY:
        {json.dumps(column_summary, indent=2)}
        
        Based on the column types, data patterns, and business context, determine:
        
        1. What is the primary business purpose of this table?
        2. What type of data source is this likely from?
        3. What is the primary entity being tracked (person, company, etc.)?
        4. Overall data quality assessment
        5. Potential for enrichment and enhancement
        
        Consider common business scenarios like:
        - CRM contact exports
        - Email marketing lists
        - Lead generation data
        - Customer databases
        - Event registrations
        - Survey responses
        
        Provide specific, actionable insights about how this data could be used and improved.
        """
        
        start_time = time.time()
        
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "table_analysis",
                    "schema": response_schema
                }
            },
            temperature=0.1
        )
        
        duration = time.time() - start_time
        response_content = response.choices[0].message.content
        
        # Log the prompt and response
        log_prompt_response(
            self.logger, prompt, response_content,
            model="gpt-4o-mini", duration=duration
        )
        
        return json.loads(response_content)
    
    def _get_fallback_column_analysis(self, col_name: str) -> Dict[str, Any]:
        """Provide fallback analysis when API call fails"""
        return {
            "semantic_type": "unknown",
            "data_format": "unknown", 
            "business_value": "low",
            "data_quality": "unknown",
            "standardization_needs": f"Analysis failed for {col_name}",
            "potential_enrichment": "Manual review needed",
            "total_count": 0,
            "non_null_count": 0,
            "unique_count": 0,
            "completeness": 0
        }
    
    def _log_analysis_results(self, analysis: Dict[str, Any]):
        """Log comprehensive analysis results"""
        
        self.logger.info("✅ Semantic Analysis Complete:")
        self.logger.info(f"🎯 Business purpose: {analysis.get('table_purpose', 'Unknown')}")
        self.logger.info(f"📦 Identified source: {analysis.get('data_source', 'Unknown')}")
        
        # Log confidence and quality metrics
        confidence = analysis.get('confidence', 0)
        if confidence:
            self.logger.info(f"🎯 Analysis confidence: {confidence:.2f}")
        
        # Platform/export detection
        data_source = analysis.get('data_source', '')
        if 'export' in data_source.lower():
            platform = confidence
            export_type = data_source
            self.logger.info(f"🔍 Platform detection: {platform:.2f} confidence ({export_type})")
        
        # Quality assessment
        quality_assessment = analysis.get('quality_assessment', {})
        if quality_assessment:
            completeness = quality_assessment.get('overall_completeness', 0)
            consistency = quality_assessment.get('data_consistency', 0)
            self.logger.info(f"📊 Data quality: {completeness:.2%} complete, {consistency:.2%} consistent")
        
        # Column type distribution
        if 'column_semantics' in analysis:
            column_types = []
            for col_data in analysis['column_semantics'].values():
                column_types.append(col_data.get('semantic_type', 'unknown'))
            
            type_counts = {}
            for col_type in column_types:
                type_counts[col_type] = type_counts.get(col_type, 0) + 1
            
            self.logger.info(f"📋 Column types detected: {dict(type_counts)}",
                           extra={'column_type_distribution': type_counts})
        
        # Log detailed analysis for debugging
        self.logger.debug("Complete analysis results", extra={'analysis': analysis}) 