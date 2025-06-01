# InTabular Test Suite

This directory contains comprehensive tests for the InTabular intelligent CSV ingestion system across diverse data domains.

## 🚀 Quick Start

```bash
# Run essential tests (fastest feedback)
python test/run_essential_tests.py

# Run all fast tests (no API key needed)
pytest test/ -m "no_llm"

# Run full test suite (requires OpenAI API key)
pytest test/ -v
```

## Test Structure

```
test/
├── conftest.py                      # Pytest configuration and shared fixtures
├── data/                            # Test data files across multiple domains
│   ├── csv/                         # Sample CSV files for testing
│   │   ├── perfect_match.csv            # CSV with perfect column name matches (customer data)
│   │   ├── format_transform.csv         # CSV requiring format transformations (customer data)
│   │   ├── llm_complex.csv              # CSV requiring LLM parsing (customer data)
│   │   ├── edge_cases.csv               # CSV with edge cases and issues
│   │   ├── industrial_sensors.csv       # Industrial sensor measurement data
│   │   ├── financial_transactions.csv   # Financial transaction records
│   │   └── lab_results.csv              # Laboratory/scientific analysis data
│   ├── configs/                     # YAML configuration files
│   │   ├── customer_crm.yaml            # Full CRM configuration
│   │   ├── simple_contacts.yaml         # Simple contact management
│   │   ├── minimal_schema.yaml          # Minimal test configuration
│   │   └── industrial_monitoring.yaml   # Industrial sensor monitoring
│   └── output/                      # Test output files (auto-cleaned)
├── test_fast_core.py                # Essential fast tests for core functionality
├── test_perfect_matching.py         # Tests for perfect column matches
├── test_format_transformations.py   # Tests for format transformations
├── test_llm_parsing.py              # Tests for LLM-based parsing
├── test_edge_cases.py               # Tests for edge cases and errors
├── test_integration.py              # End-to-end integration tests
├── test_diverse_domains.py          # Tests for non-customer data domains
├── requirements.txt                 # Test dependencies
├── pytest.ini                      # Pytest configuration
├── run_essential_tests.py           # Essential test runner (fast feedback)
├── run_tests.py                     # Comprehensive demo test runner
└── README.md                       # This file
```

## Running Tests

### Prerequisites

1. **Install test dependencies:**
   ```bash
   pip install -r test/requirements.txt
   ```

2. **Set up OpenAI API key** (for LLM tests):
   ```bash
   export OPENAI_API_KEY="your-api-key-here"
   ```

### Essential Tests (Super Fast)

For quick development feedback, run only the most critical tests:

```bash
# Run essential tests (~5 seconds)
python test/run_essential_tests.py

# Or manually:
pytest test/test_fast_core.py -v
```

These tests cover:
- ✅ Core transformation functionality
- ✅ Data loading and configuration
- ✅ Safety restrictions and error handling
- ✅ Mock infrastructure validation

### Fast Tests (No LLM/API Required)

These tests run quickly without external dependencies:

```bash
# Run all fast tests
pytest test/ -m "no_llm"

# Run fast tests in parallel for speed
pytest test/ -m "no_llm" -n auto

# Fast tests by domain
pytest test/ -m "industrial and no_llm"
pytest test/ -m "financial and no_llm"
pytest test/ -m "scientific and no_llm"
pytest test/ -m "customer and no_llm"
```

### LLM Tests (Require OpenAI API Key)

These tests require an OpenAI API key and test the full pipeline:

```bash
# Run all LLM tests
pytest test/ -m "llm"

# Skip LLM tests
pytest test/ -m "not llm"

# LLM tests by domain
pytest test/ -m "industrial and llm"
pytest test/ -m "customer and llm"
```

### Domain-Specific Testing

Test specific data domains independently:

```bash
# Industrial sensor monitoring
pytest test/ -m "industrial" -v

# Financial transaction processing
pytest test/ -m "financial" -v

# Scientific/laboratory data
pytest test/ -m "scientific" -v

# Customer/CRM data (original domain)
pytest test/ -m "customer" -v
```

### Test Categories

#### Essential Tests (Super Fast - <10 seconds)
```bash
python test/run_essential_tests.py
pytest test/test_fast_core.py
```

#### Unit Tests (Fast, No External Dependencies)
```bash
pytest test/ -m "unit"
pytest test/ -m "no_llm"  # Equivalent for most cases
```

#### Integration Tests (May Require API Key)
```bash
pytest test/ -m "integration"
```

### Coverage Reports

```bash
# Generate coverage report
pytest test/ --cov=intabular --cov-report=html

# View coverage in browser
open test/coverage_html/index.html
```

### Parallel Execution

```bash
# Run tests in parallel (faster)
pytest test/ -n auto
```

## Test Data Domains

### Customer/CRM Data (Original Domain)
- **Files**: `perfect_match.csv`, `format_transform.csv`, `llm_complex.csv`, `edge_cases.csv`
- **Config**: `customer_crm.yaml`, `simple_contacts.yaml`
- **Focus**: Email addresses, names, companies, deals, contact information

### Industrial Sensor Monitoring
- **Files**: `industrial_sensors.csv`
- **Config**: `industrial_monitoring.yaml` 
- **Focus**: Temperature, pressure, flow rates, sensor IDs, locations, operational status
- **Tests**: Numeric data handling, unit conversions, device identification

### Financial Transactions
- **Files**: `financial_transactions.csv`
- **Focus**: Transaction IDs, account numbers, monetary amounts, transaction types
- **Tests**: Currency formatting, account masking, ID normalization

### Scientific/Laboratory Data
- **Files**: `lab_results.csv`
- **Focus**: Sample IDs, pH levels, conductivity, coordinates, measurement precision
- **Tests**: Scientific notation, coordinate formatting, precision standardization

## Test Types by Domain

### Essential Core Tests (`test_fast_core.py`)
- Core transformation operations
- Safety and security validation
- Data loading verification
- Configuration validation
- Error handling patterns

### Industrial Data Tests (`test_diverse_domains.py::TestIndustrialData`)
- Sensor data structure validation
- Temperature unit conversions (F↔C↔K)
- Sensor ID format standardization
- Full pipeline integration with real sensor data

### Financial Data Tests (`test_diverse_domains.py::TestFinancialData`) 
- Transaction ID normalization
- Monetary amount formatting
- Account number masking for security
- Currency symbol handling

### Scientific Data Tests (`test_diverse_domains.py::TestScientificData`)
- Sample ID standardization 
- GPS coordinate formatting
- Measurement precision control
- pH range validation

### Cross-Domain Tests (`test_diverse_domains.py::TestCrossDomainCapabilities`)
- Numeric data handling across all domains
- Data integrity validation
- Cross-domain analysis capabilities

## Test Markers and Filtering

### Available Markers
- `unit`: Unit tests for individual components (no external dependencies)
- `integration`: Integration tests for full pipeline (may require API key)
- `llm`: Tests that require OpenAI API key for LLM functionality
- `no_llm`: Tests that work without any LLM/API dependencies
- `slow`: Tests that take longer than 5 seconds
- `industrial`: Tests using industrial/sensor data
- `financial`: Tests using financial transaction data
- `scientific`: Tests using laboratory/research data
- `customer`: Tests using customer/CRM data

### Advanced Filtering Examples

```bash
# All non-customer domains
pytest test/ -m "not customer"

# Fast industrial tests only
pytest test/ -m "industrial and no_llm"

# All LLM tests except customer domain
pytest test/ -m "llm and not customer"

# Integration tests for specific domain
pytest test/ -m "integration and industrial"

# Only unit tests (fastest)
pytest test/ -m "unit"
```

## Development Workflow

### 1. Before Making Changes
```bash
# Verify current setup works
python test/run_essential_tests.py
```

### 2. During Development
```bash
# Quick feedback on core functionality
python test/run_essential_tests.py

# Test specific changes
pytest test/test_format_transformations.py -v
pytest test/test_diverse_domains.py::TestIndustrialData -v
```

### 3. Before Committing
```bash
# Run all fast tests
pytest test/ -m "no_llm" -v

# Generate coverage report
pytest test/ --cov=intabular --cov-report=term-missing
```

### 4. Full Validation (CI/CD)
```bash
# Complete test suite
pytest test/ -v

# Or use the demo runner
python test/run_tests.py
```

## Demo Scripts

### Essential Test Runner
```bash
python test/run_essential_tests.py
```
- ⚡ Super fast feedback (<10 seconds)
- ✅ Validates core functionality
- 🎯 Perfect for development loop

### Comprehensive Demo
```bash
python test/run_tests.py
```
- 🌍 Demonstrates all data domains
- 🔄 Shows LLM vs non-LLM separation
- 📖 Provides usage examples

## Writing New Tests

### Adding New Data Domains

1. **Add CSV data**: Create `test/data/csv/your_domain.csv`
2. **Add configuration**: Create `test/data/configs/your_domain.yaml`
3. **Add fixtures**: Update `conftest.py` with new fixtures
4. **Add test class**: Create tests in `test_diverse_domains.py`
5. **Add markers**: Add domain marker to `pytest.ini`

### Test Naming Convention

- Test files: `test_<component>.py`
- Test classes: `Test<Component>`
- Test methods: `test_<specific_behavior>`
- Use descriptive names that include the domain being tested

### Example Domain Test

```python
class TestYourDomain:
    """Test cases for your specific data domain"""
    
    @pytest.mark.your_domain
    @pytest.mark.no_llm
    def test_data_structure(self, your_domain_df):
        """Test that your domain data has expected structure"""
        assert len(your_domain_df) > 0
        assert 'key_field' in your_domain_df.columns
    
    @pytest.mark.your_domain
    @pytest.mark.llm
    def test_full_pipeline(self, analyzer, processor, your_domain_config, your_domain_df):
        """Test complete pipeline with your domain data"""
        analysis = analyzer.analyze_dataframe_structure(your_domain_df, "Your domain test")
        # ... rest of pipeline test
```

## CI/CD Integration

This test suite is designed for continuous integration:

- **Automatic Marking**: Tests are automatically marked based on dependencies
- **Graceful Skipping**: LLM tests skip gracefully without API keys
- **Parallel Execution**: Fast tests can run in parallel
- **Coverage Requirements**: Maintains minimum 70% coverage
- **Domain Isolation**: Different domains can be tested independently
- **Essential Tests**: Super fast core validation for quick feedback

## Performance Features

- **Essential Tests**: Core functionality validation in <10 seconds
- **Fast Unit Tests**: Most tests (marked `no_llm`) run in seconds
- **Parallel Execution**: Use `-n auto` for parallel test execution
- **Selective Testing**: Run only specific domains or test types
- **Efficient Mocking**: Mock LLM calls for unit tests to avoid API costs

## Troubleshooting

### Common Issues

1. **API Key Missing**: 
   ```bash
   export OPENAI_API_KEY="your-key-here"
   # Or run only fast tests: pytest test/ -m "no_llm"
   ```

2. **Import Errors**: 
   ```bash
   pip install -e .  # Install InTabular in development mode
   ```

3. **Slow Tests**: 
   ```bash
   pytest test/ -m "no_llm" -n auto  # Run fast tests in parallel
   ```

4. **Development Feedback**: 
   ```bash
   python test/run_essential_tests.py  # Super fast core validation
   ```

### Debug Mode

```bash
# Verbose debugging
pytest test/ -s --log-cli-level=DEBUG

# Stop on first failure
pytest test/ -x

# Show slowest tests
pytest test/ --durations=10

# Essential tests with detail
pytest test/test_fast_core.py -v -s --tb=long
```

## Key Benefits

🎯 **Multi-Domain Coverage**: Tests industrial, financial, scientific, and customer data
⚡ **Fast Development**: Essential tests provide feedback in seconds  
🔄 **CI/CD Ready**: Automatic test categorization and parallel execution
📊 **Comprehensive**: 70+ test cases covering edge cases and integrations
🛡️ **Production Ready**: Validates real-world data transformation scenarios
🎨 **Flexible**: Test specific domains, components, or the full pipeline
🚀 **Developer Friendly**: Multiple test runners for different development phases