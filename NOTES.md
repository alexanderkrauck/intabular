        """
        Apply function to iterable items in parallel with optional retry support.
    
        Args:
            func: Function to apply to each item
            items: Iterable of items to process
            max_workers: Maximum parallel workers
            timeout: Timeout per item in seconds
            retries: Number of retry attempts on failure (0 = no retries)
    
        Returns:
            List of func(item) results in same order as input
    
        Raises:
            Exception: If any item fails after all retry attempts
        """
        items_list = list(items)  # Convert to list to preserve order
        results = [None] * len(items_list)
    
        def _retry_wrapper(item):
            """Wrapper that handles retries for individual function calls"""
            last_exception = None
    
            for attempt in range(retries + 1):  # +1 because retries=0 means 1 attempt
                try:
                    return func(item)
                except Exception as e:
                    last_exception = e
                    if attempt < retries:  # Don't sleep on final attempt
                        # Exponential backoff: 1s, 2s, 4s, etc.
                        sleep_time = 2 ** attempt
                        time.sleep(sleep_time)
                        continue
                    else:
                        # All attempts exhausted, re-raise the last exception
                        raise last_exception
    
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks with their index
            future_to_index = {
                executor.submit(_retry_wrapper, item): idx
                for idx, item in enumerate(items_list)
            }
    
            # Collect results as they complete
            for future in as_completed(future_to_index, timeout=timeout * len(items_list)):
                idx = future_to_index[future]
                try:
>                   results[idx] = future.result(timeout=timeout)

intabular/core/utils.py:59: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
../.pyenv/versions/3.10.0/lib/python3.10/concurrent/futures/_base.py:438: in result
    return self.__get_result()
../.pyenv/versions/3.10.0/lib/python3.10/concurrent/futures/_base.py:390: in __get_result
    raise self._exception
../.pyenv/versions/3.10.0/lib/python3.10/concurrent/futures/thread.py:52: in run
    result = self.fn(*self.args, **self.kwargs)
intabular/core/utils.py:46: in _retry_wrapper
    raise last_exception
intabular/core/utils.py:36: in _retry_wrapper
    return func(item)
intabular/core/strategy.py:43: in <lambda>
    lambda target_col: (target_col, self._create_no_merge_column_mappings(target_col, target_config, dataframe_analysis)),
intabular/core/strategy.py:178: in _create_no_merge_column_mappings
    response = log_llm_call(
intabular/core/llm_logger.py:29: in log_llm_call
    response = call_func()
intabular/core/strategy.py:179: in <lambda>
    lambda: self.client.chat.completions.create(**llm_kwargs),
../.pyenv/versions/3.10.0/lib/python3.10/unittest/mock.py:1105: in __call__
    return self._mock_call(*args, **kwargs)
../.pyenv/versions/3.10.0/lib/python3.10/unittest/mock.py:1109: in _mock_call
    return self._execute_mock_call(*args, **kwargs)
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <Mock name='mock.chat.completions.create' id='4602330416'>, args = ()
kwargs = {'messages': [{'content': 'You are creating a data transformation strategy for entity identifier columns in a database...es': [...], ...}}, 'required': ['reasoning', 'transformation_type'], ...}}, 'type': 'json_schema'}, 'temperature': 0.1}
effect = <list_iterator object at 0x11251d930>

    def _execute_mock_call(self, /, *args, **kwargs):
        # separate from _increment_mock_call so that awaited functions are
        # executed separately from their call, also AsyncMock overrides this method
    
        effect = self.side_effect
        if effect is not None:
            if _is_exception(effect):
                raise effect
            elif not _callable(effect):
>               result = next(effect)
E               StopIteration

../.pyenv/versions/3.10.0/lib/python3.10/unittest/mock.py:1166: StopIteration

During handling of the above exception, another exception occurred:

self = <test_llm_parsing.TestLLMParsing object at 0x110ddfa90>
mock_strategy_creator = <intabular.core.strategy.DataframeIngestionStrategy object at 0x11217a860>
customer_crm_config = <intabular.core.config.GatekeeperConfig object at 0x112111ed0>

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
    
        mock_strategy_creator.client.chat.completions.create.return_value = mock_response
    
        # Create strategy
>       strategy = mock_strategy_creator.create_ingestion_strategy(customer_crm_config, mock_analysis)

test/test_llm_parsing.py:55: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
intabular/core/strategy.py:42: in create_ingestion_strategy
    no_merge_column_mappings = dict(parallel_map(
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

func = <function DataframeIngestionStrategy.create_ingestion_strategy.<locals>.<lambda> at 0x1120becb0>
items = ['email', 'full_name', 'company_name', 'deal_stage', 'deal_value', 'phone', ...], max_workers = 5, timeout = 30, retries = 3

    def parallel_map(func: Callable, items: Iterable, max_workers: int = 5, timeout: int = 30, retries: int = 0) -> List:
        """
        Apply function to iterable items in parallel with optional retry support.
    
        Args:
            func: Function to apply to each item
            items: Iterable of items to process
            max_workers: Maximum parallel workers
            timeout: Timeout per item in seconds
            retries: Number of retry attempts on failure (0 = no retries)
    
        Returns:
            List of func(item) results in same order as input
    
        Raises:
            Exception: If any item fails after all retry attempts
        """
        items_list = list(items)  # Convert to list to preserve order
        results = [None] * len(items_list)
    
        def _retry_wrapper(item):
            """Wrapper that handles retries for individual function calls"""
            last_exception = None
    
            for attempt in range(retries + 1):  # +1 because retries=0 means 1 attempt
                try:
                    return func(item)
                except Exception as e:
                    last_exception = e
                    if attempt < retries:  # Don't sleep on final attempt
                        # Exponential backoff: 1s, 2s, 4s, etc.
                        sleep_time = 2 ** attempt
                        time.sleep(sleep_time)
                        continue
                    else:
                        # All attempts exhausted, re-raise the last exception
                        raise last_exception
    
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks with their index
            future_to_index = {
                executor.submit(_retry_wrapper, item): idx
                for idx, item in enumerate(items_list)
            }
    
            # Collect results as they complete
            for future in as_completed(future_to_index, timeout=timeout * len(items_list)):
                idx = future_to_index[future]
                try:
                    results[idx] = future.result(timeout=timeout)
                except Exception as e:
>                   raise Exception(f"Failed processing item {idx} ({items_list[idx]}) after {retries + 1} attempts: {e}")
E                   Exception: Failed processing item 1 (full_name) after 4 attempts:

intabular/core/utils.py:61: Exception
-------------------------------------------------------------------- Captured log setup --------------------------------------------------------------------
INFO     intabular.config:config.py:129 Loading configuration from /Users/alexanderkrauck/MailPipe/test/data/configs/customer_crm.yaml
INFO     intabular.config:config.py:147 Configuration loaded successfully
-------------------------------------------------------------------- Captured log call ---------------------------------------------------------------------
INFO     intabular.strategy:strategy.py:38 Creating entity-aware ingestion strategy...
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column email using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column full_name using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column company_name using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column deal_stage using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column deal_value using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column phone using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column full_name using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column email using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column phone using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column deal_stage using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column deal_value using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column full_name using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column phone using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column deal_value using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column email using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column deal_stage using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column full_name using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column phone using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column deal_value using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column email using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column deal_stage using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column location using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column notes using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column notes using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column location using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column notes using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column location using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column notes using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column location using dataframe analysis {'contact_info': {'data_type': 'text', 'purpose': 'Contains mixed contact data'}, 'business_entity': {'data_type': 'identifier', 'purpose': 'Company information'}, 'irrelevant_field': {'data_type': 'text', 'purpose': 'Not relevant data'}}
__________________________________________________________ TestLLMParsing.test_llm_direct_parsing __________________________________________________________

self = <test_llm_parsing.TestLLMParsing object at 0x110ddf940>, processor = <intabular.core.processor.DataframeIngestionProcessor object at 0x11271fe80>
customer_crm_config = <intabular.core.config.GatekeeperConfig object at 0x11271fd90>

    @pytest.mark.llm
    def test_llm_direct_parsing(self, processor, customer_crm_config):
        """Test direct LLM parsing without intermediate transformation"""
        # Create a mock LLM response
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "john.doe@acme.com"
    
>       processor.client.chat.completions.create.return_value = mock_response
E       AttributeError: 'method' object has no attribute 'return_value'

test/test_llm_parsing.py:68: AttributeError
-------------------------------------------------------------------- Captured log setup --------------------------------------------------------------------
INFO     intabular.config:config.py:129 Loading configuration from /Users/alexanderkrauck/MailPipe/test/data/configs/customer_crm.yaml
INFO     intabular.config:config.py:147 Configuration loaded successfully
_________________________________________________________ TestLLMParsing.test_llm_column_filtering _________________________________________________________

self = <test_llm_parsing.TestLLMParsing object at 0x110dde170>, processor = <intabular.core.processor.DataframeIngestionProcessor object at 0x112771c00>
customer_crm_config = <intabular.core.config.GatekeeperConfig object at 0x1127735b0>

    @pytest.mark.llm
    def test_llm_column_filtering(self, processor, customer_crm_config):
        """Test that LLM only receives specified source columns"""
        # Mock LLM response
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "Acme Corporation"
    
>       processor.client.chat.completions.create.return_value = mock_response
E       AttributeError: 'method' object has no attribute 'return_value'

test/test_llm_parsing.py:108: AttributeError
-------------------------------------------------------------------- Captured log setup --------------------------------------------------------------------
INFO     intabular.config:config.py:129 Loading configuration from /Users/alexanderkrauck/MailPipe/test/data/configs/customer_crm.yaml
INFO     intabular.config:config.py:147 Configuration loaded successfully
__________________________________________________ TestLLMParsing.test_llm_all_columns_when_not_specified __________________________________________________

self = <test_llm_parsing.TestLLMParsing object at 0x110ddc310>, processor = <intabular.core.processor.DataframeIngestionProcessor object at 0x11271ec80>
customer_crm_config = <intabular.core.config.GatekeeperConfig object at 0x11271ed70>

    @pytest.mark.llm
    def test_llm_all_columns_when_not_specified(self, processor, customer_crm_config):
        """Test that LLM receives all columns when llm_source_columns not specified"""
        # Mock LLM response
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "john doe"
    
>       processor.client.chat.completions.create.return_value = mock_response
E       AttributeError: 'method' object has no attribute 'return_value'

test/test_llm_parsing.py:150: AttributeError
-------------------------------------------------------------------- Captured log setup --------------------------------------------------------------------
INFO     intabular.config:config.py:129 Loading configuration from /Users/alexanderkrauck/MailPipe/test/data/configs/customer_crm.yaml
INFO     intabular.config:config.py:147 Configuration loaded successfully
____________________________________________________ TestLLMParsing.test_llm_with_current_value_merging ____________________________________________________

self = <test_llm_parsing.TestLLMParsing object at 0x110ddffa0>, processor = <intabular.core.processor.DataframeIngestionProcessor object at 0x1127738b0>
customer_crm_config = <intabular.core.config.GatekeeperConfig object at 0x110806e00>

    @pytest.mark.llm
    def test_llm_with_current_value_merging(self, processor, customer_crm_config):
        """Test LLM parsing with current value for merging"""
        # Mock LLM response that considers current value
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "Existing notes | New: Very interested in enterprise solution"
    
>       processor.client.chat.completions.create.return_value = mock_response
E       AttributeError: 'method' object has no attribute 'return_value'

test/test_llm_parsing.py:207: AttributeError
-------------------------------------------------------------------- Captured log setup --------------------------------------------------------------------
INFO     intabular.config:config.py:129 Loading configuration from /Users/alexanderkrauck/MailPipe/test/data/configs/customer_crm.yaml
INFO     intabular.config:config.py:147 Configuration loaded successfully
_____________________________________________________ TestLLMParsing.test_data_type_formatting_for_llm _____________________________________________________

self = <test_llm_parsing.TestLLMParsing object at 0x110dfc220>, processor = <intabular.core.processor.DataframeIngestionProcessor object at 0x11271eda0>
customer_crm_config = <intabular.core.config.GatekeeperConfig object at 0x1120a0730>

    @pytest.mark.llm
    def test_data_type_formatting_for_llm(self, processor, customer_crm_config):
        """Test that data types are properly formatted for LLM consumption"""
        # Mock LLM response
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "50000"
    
>       processor.client.chat.completions.create.return_value = mock_response
E       AttributeError: 'method' object has no attribute 'return_value'

test/test_llm_parsing.py:242: AttributeError
-------------------------------------------------------------------- Captured log setup --------------------------------------------------------------------
INFO     intabular.config:config.py:129 Loading configuration from /Users/alexanderkrauck/MailPipe/test/data/configs/customer_crm.yaml
INFO     intabular.config:config.py:147 Configuration loaded successfully
_________________________________________________ TestPerfectMatching.test_perfect_match_strategy_creation _________________________________________________

self = <test_perfect_matching.TestPerfectMatching object at 0x110dfc940>, analyzer = <intabular.core.analyzer.DataframeAnalyzer object at 0x112772140>
strategy_creator = <intabular.core.strategy.DataframeIngestionStrategy object at 0x112773310>
customer_crm_config = <intabular.core.config.GatekeeperConfig object at 0x1127734c0>
perfect_match_df =                         email    full_name        company_name  ...         phone                location             ...wn  enterprise systems  ...  555-444-3333        chicago, il, usa     signed contract last month 

[4 rows x 8 columns]

    def test_perfect_match_strategy_creation(self, analyzer, strategy_creator, customer_crm_config, perfect_match_df):
        """Test that perfect matches create appropriate format strategies"""
        # Analyze the perfect match dataframe
        analysis = analyzer.analyze_dataframe_structure(perfect_match_df, "Perfect match test data")
    
        # Create strategy
        strategy = strategy_creator.create_ingestion_strategy(customer_crm_config, analysis)
    
        # All entity columns should have some transformation type (not 'none')
        entity_mappings = strategy.no_merge_column_mappings
        for col_name, mapping in entity_mappings.items():
            if col_name in customer_crm_config.entity_columns:
>               assert mapping['transformation_type'] in ['format', 'llm_format'], \
                    f"Entity column {col_name} should have a valid transformation"
E               AssertionError: Entity column company_name should have a valid transformation
E               assert 'none' in ['format', 'llm_format']

test/test_perfect_matching.py:24: AssertionError
-------------------------------------------------------------------- Captured log setup --------------------------------------------------------------------
INFO     intabular.config:config.py:129 Loading configuration from /Users/alexanderkrauck/MailPipe/test/data/configs/customer_crm.yaml
INFO     intabular.config:config.py:147 Configuration loaded successfully
-------------------------------------------------------------------- Captured log call ---------------------------------------------------------------------
INFO     intabular.analyzer:analyzer.py:55 Starting dataframe analysis
INFO     intabular.analyzer:analyzer.py:81 DataFrame: 4 rows × 8 columns
INFO     intabular.analyzer:analyzer.py:82 Purpose: Sales pipeline data capturing potential client information and deal status.
INFO     intabular.strategy:strategy.py:38 Creating entity-aware ingestion strategy...
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column email using dataframe analysis {'email': {'data_type': 'identifier', 'purpose': 'The email column serves as a unique identifier for users or contacts, allowing for distinct identification and communication within the database.', 'reasoning': 'The column contains structured email addresses, which are used to identify individuals or entities uniquely. It does not contain free-form text but rather follows a specific format that qualifies it as an identifier.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'full_name': {'data_type': 'text', 'purpose': "The 'full_name' column contains free-form text representing the names of individuals, which is essential for identifying and distinguishing between different records in the database.", 'reasoning': "The column contains names in a free-form text format, which does not conform to structured identifiers like emails or IDs, thus it is classified as 'text'.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'company_name': {'data_type': 'text', 'purpose': "The 'company_name' column contains the names of companies, which are free-form text entries. This information is essential for identifying and categorizing the entities represented in the database, allowing for better organization and retrieval of company-related data.", 'reasoning': 'The column contains names of companies, which are not structured identifiers but rather descriptive text entries. The uniqueness and completeness statistics indicate that all entries are valid and distinct.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_stage': {'data_type': 'text', 'purpose': "The 'deal_stage' column provides qualitative information about the current status of a deal in a sales process, which is essential for understanding the sales pipeline and making informed business decisions.", 'reasoning': "The values in the 'deal_stage' column represent stages in a sales process and are descriptive in nature, indicating the progress of deals. Therefore, it is classified as 'text' rather than an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_value': {'data_type': 'identifier', 'purpose': "The 'deal_value' column contains numerical values representing monetary amounts associated with deals, which can be used for financial analysis, reporting, and decision-making within the database.", 'reasoning': "The values in the 'deal_value' column are structured numerical representations of monetary amounts, making it suitable for classification as an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'phone': {'data_type': 'identifier', 'purpose': "The 'phone' column contains structured references to phone numbers, which can be used for contact purposes, ensuring that each entry is a valid identifier for individuals or entities in the database.", 'reasoning': "The values in the 'phone' column are formatted as phone numbers, which are structured identifiers rather than free-form text. This classification is appropriate as they serve a specific purpose in identifying contacts.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'location': {'data_type': 'text', 'purpose': "The 'location' column provides descriptive information about geographical locations, which can be useful for categorizing data, analyzing trends based on geography, or filtering records based on location.", 'reasoning': "The values in the 'location' column are free-form text descriptions of geographical locations, indicating cities and states, rather than structured identifiers.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'notes': {'data_type': 'text', 'purpose': "The 'notes' column contains free-form text entries that provide qualitative insights into customer interactions and interests, which can be valuable for understanding customer engagement and follow-up actions.", 'reasoning': "The content of the 'notes' column consists of descriptive statements about customer interactions, making it free-form text rather than structured data.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column full_name using dataframe analysis {'email': {'data_type': 'identifier', 'purpose': 'The email column serves as a unique identifier for users or contacts, allowing for distinct identification and communication within the database.', 'reasoning': 'The column contains structured email addresses, which are used to identify individuals or entities uniquely. It does not contain free-form text but rather follows a specific format that qualifies it as an identifier.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'full_name': {'data_type': 'text', 'purpose': "The 'full_name' column contains free-form text representing the names of individuals, which is essential for identifying and distinguishing between different records in the database.", 'reasoning': "The column contains names in a free-form text format, which does not conform to structured identifiers like emails or IDs, thus it is classified as 'text'.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'company_name': {'data_type': 'text', 'purpose': "The 'company_name' column contains the names of companies, which are free-form text entries. This information is essential for identifying and categorizing the entities represented in the database, allowing for better organization and retrieval of company-related data.", 'reasoning': 'The column contains names of companies, which are not structured identifiers but rather descriptive text entries. The uniqueness and completeness statistics indicate that all entries are valid and distinct.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_stage': {'data_type': 'text', 'purpose': "The 'deal_stage' column provides qualitative information about the current status of a deal in a sales process, which is essential for understanding the sales pipeline and making informed business decisions.", 'reasoning': "The values in the 'deal_stage' column represent stages in a sales process and are descriptive in nature, indicating the progress of deals. Therefore, it is classified as 'text' rather than an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_value': {'data_type': 'identifier', 'purpose': "The 'deal_value' column contains numerical values representing monetary amounts associated with deals, which can be used for financial analysis, reporting, and decision-making within the database.", 'reasoning': "The values in the 'deal_value' column are structured numerical representations of monetary amounts, making it suitable for classification as an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'phone': {'data_type': 'identifier', 'purpose': "The 'phone' column contains structured references to phone numbers, which can be used for contact purposes, ensuring that each entry is a valid identifier for individuals or entities in the database.", 'reasoning': "The values in the 'phone' column are formatted as phone numbers, which are structured identifiers rather than free-form text. This classification is appropriate as they serve a specific purpose in identifying contacts.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'location': {'data_type': 'text', 'purpose': "The 'location' column provides descriptive information about geographical locations, which can be useful for categorizing data, analyzing trends based on geography, or filtering records based on location.", 'reasoning': "The values in the 'location' column are free-form text descriptions of geographical locations, indicating cities and states, rather than structured identifiers.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'notes': {'data_type': 'text', 'purpose': "The 'notes' column contains free-form text entries that provide qualitative insights into customer interactions and interests, which can be valuable for understanding customer engagement and follow-up actions.", 'reasoning': "The content of the 'notes' column consists of descriptive statements about customer interactions, making it free-form text rather than structured data.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column company_name using dataframe analysis {'email': {'data_type': 'identifier', 'purpose': 'The email column serves as a unique identifier for users or contacts, allowing for distinct identification and communication within the database.', 'reasoning': 'The column contains structured email addresses, which are used to identify individuals or entities uniquely. It does not contain free-form text but rather follows a specific format that qualifies it as an identifier.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'full_name': {'data_type': 'text', 'purpose': "The 'full_name' column contains free-form text representing the names of individuals, which is essential for identifying and distinguishing between different records in the database.", 'reasoning': "The column contains names in a free-form text format, which does not conform to structured identifiers like emails or IDs, thus it is classified as 'text'.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'company_name': {'data_type': 'text', 'purpose': "The 'company_name' column contains the names of companies, which are free-form text entries. This information is essential for identifying and categorizing the entities represented in the database, allowing for better organization and retrieval of company-related data.", 'reasoning': 'The column contains names of companies, which are not structured identifiers but rather descriptive text entries. The uniqueness and completeness statistics indicate that all entries are valid and distinct.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_stage': {'data_type': 'text', 'purpose': "The 'deal_stage' column provides qualitative information about the current status of a deal in a sales process, which is essential for understanding the sales pipeline and making informed business decisions.", 'reasoning': "The values in the 'deal_stage' column represent stages in a sales process and are descriptive in nature, indicating the progress of deals. Therefore, it is classified as 'text' rather than an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_value': {'data_type': 'identifier', 'purpose': "The 'deal_value' column contains numerical values representing monetary amounts associated with deals, which can be used for financial analysis, reporting, and decision-making within the database.", 'reasoning': "The values in the 'deal_value' column are structured numerical representations of monetary amounts, making it suitable for classification as an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'phone': {'data_type': 'identifier', 'purpose': "The 'phone' column contains structured references to phone numbers, which can be used for contact purposes, ensuring that each entry is a valid identifier for individuals or entities in the database.", 'reasoning': "The values in the 'phone' column are formatted as phone numbers, which are structured identifiers rather than free-form text. This classification is appropriate as they serve a specific purpose in identifying contacts.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'location': {'data_type': 'text', 'purpose': "The 'location' column provides descriptive information about geographical locations, which can be useful for categorizing data, analyzing trends based on geography, or filtering records based on location.", 'reasoning': "The values in the 'location' column are free-form text descriptions of geographical locations, indicating cities and states, rather than structured identifiers.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'notes': {'data_type': 'text', 'purpose': "The 'notes' column contains free-form text entries that provide qualitative insights into customer interactions and interests, which can be valuable for understanding customer engagement and follow-up actions.", 'reasoning': "The content of the 'notes' column consists of descriptive statements about customer interactions, making it free-form text rather than structured data.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column deal_stage using dataframe analysis {'email': {'data_type': 'identifier', 'purpose': 'The email column serves as a unique identifier for users or contacts, allowing for distinct identification and communication within the database.', 'reasoning': 'The column contains structured email addresses, which are used to identify individuals or entities uniquely. It does not contain free-form text but rather follows a specific format that qualifies it as an identifier.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'full_name': {'data_type': 'text', 'purpose': "The 'full_name' column contains free-form text representing the names of individuals, which is essential for identifying and distinguishing between different records in the database.", 'reasoning': "The column contains names in a free-form text format, which does not conform to structured identifiers like emails or IDs, thus it is classified as 'text'.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'company_name': {'data_type': 'text', 'purpose': "The 'company_name' column contains the names of companies, which are free-form text entries. This information is essential for identifying and categorizing the entities represented in the database, allowing for better organization and retrieval of company-related data.", 'reasoning': 'The column contains names of companies, which are not structured identifiers but rather descriptive text entries. The uniqueness and completeness statistics indicate that all entries are valid and distinct.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_stage': {'data_type': 'text', 'purpose': "The 'deal_stage' column provides qualitative information about the current status of a deal in a sales process, which is essential for understanding the sales pipeline and making informed business decisions.", 'reasoning': "The values in the 'deal_stage' column represent stages in a sales process and are descriptive in nature, indicating the progress of deals. Therefore, it is classified as 'text' rather than an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_value': {'data_type': 'identifier', 'purpose': "The 'deal_value' column contains numerical values representing monetary amounts associated with deals, which can be used for financial analysis, reporting, and decision-making within the database.", 'reasoning': "The values in the 'deal_value' column are structured numerical representations of monetary amounts, making it suitable for classification as an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'phone': {'data_type': 'identifier', 'purpose': "The 'phone' column contains structured references to phone numbers, which can be used for contact purposes, ensuring that each entry is a valid identifier for individuals or entities in the database.", 'reasoning': "The values in the 'phone' column are formatted as phone numbers, which are structured identifiers rather than free-form text. This classification is appropriate as they serve a specific purpose in identifying contacts.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'location': {'data_type': 'text', 'purpose': "The 'location' column provides descriptive information about geographical locations, which can be useful for categorizing data, analyzing trends based on geography, or filtering records based on location.", 'reasoning': "The values in the 'location' column are free-form text descriptions of geographical locations, indicating cities and states, rather than structured identifiers.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'notes': {'data_type': 'text', 'purpose': "The 'notes' column contains free-form text entries that provide qualitative insights into customer interactions and interests, which can be valuable for understanding customer engagement and follow-up actions.", 'reasoning': "The content of the 'notes' column consists of descriptive statements about customer interactions, making it free-form text rather than structured data.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column deal_value using dataframe analysis {'email': {'data_type': 'identifier', 'purpose': 'The email column serves as a unique identifier for users or contacts, allowing for distinct identification and communication within the database.', 'reasoning': 'The column contains structured email addresses, which are used to identify individuals or entities uniquely. It does not contain free-form text but rather follows a specific format that qualifies it as an identifier.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'full_name': {'data_type': 'text', 'purpose': "The 'full_name' column contains free-form text representing the names of individuals, which is essential for identifying and distinguishing between different records in the database.", 'reasoning': "The column contains names in a free-form text format, which does not conform to structured identifiers like emails or IDs, thus it is classified as 'text'.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'company_name': {'data_type': 'text', 'purpose': "The 'company_name' column contains the names of companies, which are free-form text entries. This information is essential for identifying and categorizing the entities represented in the database, allowing for better organization and retrieval of company-related data.", 'reasoning': 'The column contains names of companies, which are not structured identifiers but rather descriptive text entries. The uniqueness and completeness statistics indicate that all entries are valid and distinct.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_stage': {'data_type': 'text', 'purpose': "The 'deal_stage' column provides qualitative information about the current status of a deal in a sales process, which is essential for understanding the sales pipeline and making informed business decisions.", 'reasoning': "The values in the 'deal_stage' column represent stages in a sales process and are descriptive in nature, indicating the progress of deals. Therefore, it is classified as 'text' rather than an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_value': {'data_type': 'identifier', 'purpose': "The 'deal_value' column contains numerical values representing monetary amounts associated with deals, which can be used for financial analysis, reporting, and decision-making within the database.", 'reasoning': "The values in the 'deal_value' column are structured numerical representations of monetary amounts, making it suitable for classification as an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'phone': {'data_type': 'identifier', 'purpose': "The 'phone' column contains structured references to phone numbers, which can be used for contact purposes, ensuring that each entry is a valid identifier for individuals or entities in the database.", 'reasoning': "The values in the 'phone' column are formatted as phone numbers, which are structured identifiers rather than free-form text. This classification is appropriate as they serve a specific purpose in identifying contacts.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'location': {'data_type': 'text', 'purpose': "The 'location' column provides descriptive information about geographical locations, which can be useful for categorizing data, analyzing trends based on geography, or filtering records based on location.", 'reasoning': "The values in the 'location' column are free-form text descriptions of geographical locations, indicating cities and states, rather than structured identifiers.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'notes': {'data_type': 'text', 'purpose': "The 'notes' column contains free-form text entries that provide qualitative insights into customer interactions and interests, which can be valuable for understanding customer engagement and follow-up actions.", 'reasoning': "The content of the 'notes' column consists of descriptive statements about customer interactions, making it free-form text rather than structured data.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column phone using dataframe analysis {'email': {'data_type': 'identifier', 'purpose': 'The email column serves as a unique identifier for users or contacts, allowing for distinct identification and communication within the database.', 'reasoning': 'The column contains structured email addresses, which are used to identify individuals or entities uniquely. It does not contain free-form text but rather follows a specific format that qualifies it as an identifier.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'full_name': {'data_type': 'text', 'purpose': "The 'full_name' column contains free-form text representing the names of individuals, which is essential for identifying and distinguishing between different records in the database.", 'reasoning': "The column contains names in a free-form text format, which does not conform to structured identifiers like emails or IDs, thus it is classified as 'text'.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'company_name': {'data_type': 'text', 'purpose': "The 'company_name' column contains the names of companies, which are free-form text entries. This information is essential for identifying and categorizing the entities represented in the database, allowing for better organization and retrieval of company-related data.", 'reasoning': 'The column contains names of companies, which are not structured identifiers but rather descriptive text entries. The uniqueness and completeness statistics indicate that all entries are valid and distinct.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_stage': {'data_type': 'text', 'purpose': "The 'deal_stage' column provides qualitative information about the current status of a deal in a sales process, which is essential for understanding the sales pipeline and making informed business decisions.", 'reasoning': "The values in the 'deal_stage' column represent stages in a sales process and are descriptive in nature, indicating the progress of deals. Therefore, it is classified as 'text' rather than an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_value': {'data_type': 'identifier', 'purpose': "The 'deal_value' column contains numerical values representing monetary amounts associated with deals, which can be used for financial analysis, reporting, and decision-making within the database.", 'reasoning': "The values in the 'deal_value' column are structured numerical representations of monetary amounts, making it suitable for classification as an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'phone': {'data_type': 'identifier', 'purpose': "The 'phone' column contains structured references to phone numbers, which can be used for contact purposes, ensuring that each entry is a valid identifier for individuals or entities in the database.", 'reasoning': "The values in the 'phone' column are formatted as phone numbers, which are structured identifiers rather than free-form text. This classification is appropriate as they serve a specific purpose in identifying contacts.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'location': {'data_type': 'text', 'purpose': "The 'location' column provides descriptive information about geographical locations, which can be useful for categorizing data, analyzing trends based on geography, or filtering records based on location.", 'reasoning': "The values in the 'location' column are free-form text descriptions of geographical locations, indicating cities and states, rather than structured identifiers.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'notes': {'data_type': 'text', 'purpose': "The 'notes' column contains free-form text entries that provide qualitative insights into customer interactions and interests, which can be valuable for understanding customer engagement and follow-up actions.", 'reasoning': "The content of the 'notes' column consists of descriptive statements about customer interactions, making it free-form text rather than structured data.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column location using dataframe analysis {'email': {'data_type': 'identifier', 'purpose': 'The email column serves as a unique identifier for users or contacts, allowing for distinct identification and communication within the database.', 'reasoning': 'The column contains structured email addresses, which are used to identify individuals or entities uniquely. It does not contain free-form text but rather follows a specific format that qualifies it as an identifier.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'full_name': {'data_type': 'text', 'purpose': "The 'full_name' column contains free-form text representing the names of individuals, which is essential for identifying and distinguishing between different records in the database.", 'reasoning': "The column contains names in a free-form text format, which does not conform to structured identifiers like emails or IDs, thus it is classified as 'text'.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'company_name': {'data_type': 'text', 'purpose': "The 'company_name' column contains the names of companies, which are free-form text entries. This information is essential for identifying and categorizing the entities represented in the database, allowing for better organization and retrieval of company-related data.", 'reasoning': 'The column contains names of companies, which are not structured identifiers but rather descriptive text entries. The uniqueness and completeness statistics indicate that all entries are valid and distinct.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_stage': {'data_type': 'text', 'purpose': "The 'deal_stage' column provides qualitative information about the current status of a deal in a sales process, which is essential for understanding the sales pipeline and making informed business decisions.", 'reasoning': "The values in the 'deal_stage' column represent stages in a sales process and are descriptive in nature, indicating the progress of deals. Therefore, it is classified as 'text' rather than an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_value': {'data_type': 'identifier', 'purpose': "The 'deal_value' column contains numerical values representing monetary amounts associated with deals, which can be used for financial analysis, reporting, and decision-making within the database.", 'reasoning': "The values in the 'deal_value' column are structured numerical representations of monetary amounts, making it suitable for classification as an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'phone': {'data_type': 'identifier', 'purpose': "The 'phone' column contains structured references to phone numbers, which can be used for contact purposes, ensuring that each entry is a valid identifier for individuals or entities in the database.", 'reasoning': "The values in the 'phone' column are formatted as phone numbers, which are structured identifiers rather than free-form text. This classification is appropriate as they serve a specific purpose in identifying contacts.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'location': {'data_type': 'text', 'purpose': "The 'location' column provides descriptive information about geographical locations, which can be useful for categorizing data, analyzing trends based on geography, or filtering records based on location.", 'reasoning': "The values in the 'location' column are free-form text descriptions of geographical locations, indicating cities and states, rather than structured identifiers.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'notes': {'data_type': 'text', 'purpose': "The 'notes' column contains free-form text entries that provide qualitative insights into customer interactions and interests, which can be valuable for understanding customer engagement and follow-up actions.", 'reasoning': "The content of the 'notes' column consists of descriptive statements about customer interactions, making it free-form text rather than structured data.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}}
INFO     intabular.strategy:strategy.py:84 Creating no merge column mapping for column notes using dataframe analysis {'email': {'data_type': 'identifier', 'purpose': 'The email column serves as a unique identifier for users or contacts, allowing for distinct identification and communication within the database.', 'reasoning': 'The column contains structured email addresses, which are used to identify individuals or entities uniquely. It does not contain free-form text but rather follows a specific format that qualifies it as an identifier.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'full_name': {'data_type': 'text', 'purpose': "The 'full_name' column contains free-form text representing the names of individuals, which is essential for identifying and distinguishing between different records in the database.", 'reasoning': "The column contains names in a free-form text format, which does not conform to structured identifiers like emails or IDs, thus it is classified as 'text'.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'company_name': {'data_type': 'text', 'purpose': "The 'company_name' column contains the names of companies, which are free-form text entries. This information is essential for identifying and categorizing the entities represented in the database, allowing for better organization and retrieval of company-related data.", 'reasoning': 'The column contains names of companies, which are not structured identifiers but rather descriptive text entries. The uniqueness and completeness statistics indicate that all entries are valid and distinct.', 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_stage': {'data_type': 'text', 'purpose': "The 'deal_stage' column provides qualitative information about the current status of a deal in a sales process, which is essential for understanding the sales pipeline and making informed business decisions.", 'reasoning': "The values in the 'deal_stage' column represent stages in a sales process and are descriptive in nature, indicating the progress of deals. Therefore, it is classified as 'text' rather than an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'deal_value': {'data_type': 'identifier', 'purpose': "The 'deal_value' column contains numerical values representing monetary amounts associated with deals, which can be used for financial analysis, reporting, and decision-making within the database.", 'reasoning': "The values in the 'deal_value' column are structured numerical representations of monetary amounts, making it suitable for classification as an identifier.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'phone': {'data_type': 'identifier', 'purpose': "The 'phone' column contains structured references to phone numbers, which can be used for contact purposes, ensuring that each entry is a valid identifier for individuals or entities in the database.", 'reasoning': "The values in the 'phone' column are formatted as phone numbers, which are structured identifiers rather than free-form text. This classification is appropriate as they serve a specific purpose in identifying contacts.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'location': {'data_type': 'text', 'purpose': "The 'location' column provides descriptive information about geographical locations, which can be useful for categorizing data, analyzing trends based on geography, or filtering records based on location.", 'reasoning': "The values in the 'location' column are free-form text descriptions of geographical locations, indicating cities and states, rather than structured identifiers.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}, 'notes': {'data_type': 'text', 'purpose': "The 'notes' column contains free-form text entries that provide qualitative insights into customer interactions and interests, which can be valuable for understanding customer engagement and follow-up actions.", 'reasoning': "The content of the 'notes' column consists of descriptive statements about customer interactions, making it free-form text rather than structured data.", 'total_count': 4, 'non_null_count': 4, 'unique_count': 4, 'completeness': 1.0}}
______________________________________________________ TestPerfectMatching.test_data_types_preserved _______________________________________________________

self = <test_perfect_matching.TestPerfectMatching object at 0x110dfd6f0>
processor = <intabular.core.processor.DataframeIngestionProcessor object at 0x11250f670>
customer_crm_config = <intabular.core.config.GatekeeperConfig object at 0x1127717b0>

    def test_data_types_preserved(self, processor, customer_crm_config):
        """Test that data types are properly handled during transformation"""
        test_data = {
            'email': ['test@example.com'],
            'full_name': ['Test User'],
            'deal_value': [50000],  # Numeric value
            'notes': ['Test notes']
        }
        test_df = pd.DataFrame(test_data)
    
        # Test execute_transformation directly
        result = processor.execute_transformation('deal_value', test_data)
>       assert result == '50000', "Numeric values should be converted to strings"
E       AssertionError: Numeric values should be converted to strings
E       assert '[50000]' == '50000'
E         
E         - 50000
E         + [50000]
E         ? +     +

test/test_perfect_matching.py:136: AssertionError
-------------------------------------------------------------------- Captured log setup --------------------------------------------------------------------
INFO     intabular.config:config.py:129 Loading configuration from /Users/alexanderkrauck/MailPipe/test/data/configs/customer_crm.yaml
INFO     intabular.config:config.py:147 Configuration loaded successfully
===================================================================== warnings summary =====================================================================
test/test_diverse_domains.py:13
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:13: PytestUnknownMarkWarning: Unknown pytest.mark.industrial - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.industrial

test/test_diverse_domains.py:14
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:14: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:27
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:27: PytestUnknownMarkWarning: Unknown pytest.mark.industrial - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.industrial

test/test_diverse_domains.py:28
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:28: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:41
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:41: PytestUnknownMarkWarning: Unknown pytest.mark.industrial - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.industrial

test/test_diverse_domains.py:42
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:42: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:55
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:55: PytestUnknownMarkWarning: Unknown pytest.mark.industrial - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.industrial

test/test_diverse_domains.py:56
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:56: PytestUnknownMarkWarning: Unknown pytest.mark.llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.llm

test/test_diverse_domains.py:80
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:80: PytestUnknownMarkWarning: Unknown pytest.mark.financial - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.financial

test/test_diverse_domains.py:81
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:81: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:99
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:99: PytestUnknownMarkWarning: Unknown pytest.mark.financial - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.financial

test/test_diverse_domains.py:100
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:100: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:113
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:113: PytestUnknownMarkWarning: Unknown pytest.mark.financial - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.financial

test/test_diverse_domains.py:114
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:114: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:127
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:127: PytestUnknownMarkWarning: Unknown pytest.mark.financial - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.financial

test/test_diverse_domains.py:128
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:128: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:144
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:144: PytestUnknownMarkWarning: Unknown pytest.mark.scientific - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.scientific

test/test_diverse_domains.py:145
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:145: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:168
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:168: PytestUnknownMarkWarning: Unknown pytest.mark.scientific - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.scientific

test/test_diverse_domains.py:169
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:169: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:182
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:182: PytestUnknownMarkWarning: Unknown pytest.mark.scientific - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.scientific

test/test_diverse_domains.py:183
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:183: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:195
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:195: PytestUnknownMarkWarning: Unknown pytest.mark.scientific - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.scientific

test/test_diverse_domains.py:196
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:196: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:213
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:213: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:234
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:234: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_diverse_domains.py:256
  /Users/alexanderkrauck/MailPipe/test/test_diverse_domains.py:256: PytestUnknownMarkWarning: Unknown pytest.mark.llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.llm

test/test_fast_core.py:15
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:15: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_fast_core.py:16
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:16: PytestUnknownMarkWarning: Unknown pytest.mark.unit - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.unit

test/test_fast_core.py:35
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:35: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_fast_core.py:36
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:36: PytestUnknownMarkWarning: Unknown pytest.mark.unit - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.unit

test/test_fast_core.py:52
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:52: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_fast_core.py:53
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:53: PytestUnknownMarkWarning: Unknown pytest.mark.unit - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.unit

test/test_fast_core.py:68
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:68: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_fast_core.py:69
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:69: PytestUnknownMarkWarning: Unknown pytest.mark.unit - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.unit

test/test_fast_core.py:88
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:88: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_fast_core.py:89
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:89: PytestUnknownMarkWarning: Unknown pytest.mark.unit - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.unit

test/test_fast_core.py:113
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:113: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_fast_core.py:114
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:114: PytestUnknownMarkWarning: Unknown pytest.mark.unit - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.unit

test/test_fast_core.py:133
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:133: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_fast_core.py:134
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:134: PytestUnknownMarkWarning: Unknown pytest.mark.unit - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.unit

test/test_fast_core.py:146
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:146: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_fast_core.py:147
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:147: PytestUnknownMarkWarning: Unknown pytest.mark.unit - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.unit

test/test_fast_core.py:167
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:167: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_fast_core.py:168
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:168: PytestUnknownMarkWarning: Unknown pytest.mark.unit - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.unit

test/test_fast_core.py:181
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:181: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_fast_core.py:182
  /Users/alexanderkrauck/MailPipe/test/test_fast_core.py:182: PytestUnknownMarkWarning: Unknown pytest.mark.unit - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.unit

test/test_llm_parsing.py:13
  /Users/alexanderkrauck/MailPipe/test/test_llm_parsing.py:13: PytestUnknownMarkWarning: Unknown pytest.mark.llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.llm

test/test_llm_parsing.py:30
  /Users/alexanderkrauck/MailPipe/test/test_llm_parsing.py:30: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_llm_parsing.py:60
  /Users/alexanderkrauck/MailPipe/test/test_llm_parsing.py:60: PytestUnknownMarkWarning: Unknown pytest.mark.llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.llm

test/test_llm_parsing.py:100
  /Users/alexanderkrauck/MailPipe/test/test_llm_parsing.py:100: PytestUnknownMarkWarning: Unknown pytest.mark.llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.llm

test/test_llm_parsing.py:142
  /Users/alexanderkrauck/MailPipe/test/test_llm_parsing.py:142: PytestUnknownMarkWarning: Unknown pytest.mark.llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.llm

test/test_llm_parsing.py:179
  /Users/alexanderkrauck/MailPipe/test/test_llm_parsing.py:179: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.no_llm

test/test_llm_parsing.py:199
  /Users/alexanderkrauck/MailPipe/test/test_llm_parsing.py:199: PytestUnknownMarkWarning: Unknown pytest.mark.llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.llm

test/test_llm_parsing.py:234
  /Users/alexanderkrauck/MailPipe/test/test_llm_parsing.py:234: PytestUnknownMarkWarning: Unknown pytest.mark.llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    @pytest.mark.llm

test/conftest.py:245
  /Users/alexanderkrauck/MailPipe/test/conftest.py:245: PytestUnknownMarkWarning: Unknown pytest.mark.industrial - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    item.add_marker(pytest.mark.industrial)

test/conftest.py:241
  /Users/alexanderkrauck/MailPipe/test/conftest.py:241: PytestUnknownMarkWarning: Unknown pytest.mark.no_llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    item.add_marker(pytest.mark.no_llm)

test/conftest.py:236
  /Users/alexanderkrauck/MailPipe/test/conftest.py:236: PytestUnknownMarkWarning: Unknown pytest.mark.llm - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    item.add_marker(pytest.mark.llm)

test/conftest.py:247
  /Users/alexanderkrauck/MailPipe/test/conftest.py:247: PytestUnknownMarkWarning: Unknown pytest.mark.financial - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    item.add_marker(pytest.mark.financial)

test/conftest.py:249
  /Users/alexanderkrauck/MailPipe/test/conftest.py:249: PytestUnknownMarkWarning: Unknown pytest.mark.scientific - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    item.add_marker(pytest.mark.scientific)

test/conftest.py:251
  /Users/alexanderkrauck/MailPipe/test/conftest.py:251: PytestUnknownMarkWarning: Unknown pytest.mark.customer - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    item.add_marker(pytest.mark.customer)

test/conftest.py:257
  /Users/alexanderkrauck/MailPipe/test/conftest.py:257: PytestUnknownMarkWarning: Unknown pytest.mark.unit - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    item.add_marker(pytest.mark.unit)

test/conftest.py:255
  /Users/alexanderkrauck/MailPipe/test/conftest.py:255: PytestUnknownMarkWarning: Unknown pytest.mark.integration - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/stable/how-to/mark.html
    item.add_marker(pytest.mark.integration)

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
================================================================= short test summary info ==================================================================
FAILED test/test_edge_cases.py::TestEdgeCases::test_empty_string_vs_none_handling - AssertionError: Failed for {'field': None}: got none, expected 
FAILED test/test_edge_cases.py::TestEdgeCases::test_nan_and_inf_handling - AssertionError: NaN should become empty string
FAILED test/test_edge_cases.py::TestEdgeCases::test_extremely_long_text_handling - AssertionError: Should handle very long text
FAILED test/test_integration.py::TestIntegration::test_full_pipeline_format_transformation - AssertionError: Email should be lowercase: 
FAILED test/test_integration.py::TestIntegration::test_error_recovery_in_pipeline - AttributeError: 'method' object has no attribute 'return_value'

================================================== 15 failed, 69 passed, 63 warnings in 356.30s (0:05:56) ==================================================
(.venv) [20:35:31] alexanderkrauck@Alexanders-MacBook-Pro:~/MailPipe% 