"""
Integration tests for the Extract and Load step of the podcast analytics pipeline.

These tests verify that the raw_events model correctly loads data from NDJSON files
and handles various scenarios according to the solution design requirements.
"""

import json
import os
import shutil
from hypothesis import given, settings

from .common import strategy_with_files, run_dbt_model, query_database


@given(strategy_with_files(num_files=1))
@settings(max_examples=3, deadline=None)
def test_single_file_load(strategy):
    """
    Test the first integration test scenario:
    Given: All events serialized to an NDJSON file and copied to the staging directory
    When: The file is copied to the data directory and the pipeline is run with all files globbed
    Then: 
    - Assert the count of loaded events
    - Assert one load_at value
    - Assert one filename value
    """
    # Extract data from the strategy
    events = strategy['events']
    data_dir = strategy['data_dir']
    loading_dir = strategy['loading_dir']
    staging_paths = strategy['staging_paths']
    db_path = os.path.join(data_dir, 'podcast_analytics.duckdb')
    
    try:
        # Copy the file from staging to loading directory
        shutil.copy2(staging_paths[0], loading_dir)
        
        # Act: Run the dbt model with all files globbed from loading directory
        dbt_vars = {
            "data_load_path": loading_dir,
            "test_db_path": db_path
        }
        run_dbt_model("raw_events", dbt_vars)
        
        # Assert: Verify the results
        
        # 1. Count of loaded events
        count_query = "SELECT COUNT(*) FROM main_bronze.raw_events"
        count_result = query_database(count_query, db_path)
        assert count_result[0][0] == len(events), f"Expected {len(events)} events, got {count_result[0][0]}"
        
        # 2. One load_at value
        load_at_query = "SELECT DISTINCT load_at FROM main_bronze.raw_events"
        load_at_result = query_database(load_at_query, db_path)
        assert len(load_at_result) == 1, f"Expected exactly 1 unique load_at timestamp, got {len(load_at_result)}"
        
        # 3. One filename value
        filename_query = "SELECT DISTINCT filename FROM main_bronze.raw_events"
        filename_result = query_database(filename_query, db_path)
        assert len(filename_result) == 1, f"Expected exactly 1 unique filename, got {len(filename_result)}"
        
    finally:
        # Clean up the temporary directory created for this example
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)


@given(strategy_with_files(num_files=1))
@settings(max_examples=3, deadline=None)
def test_single_file_load_twice(strategy):
    """
    Test the second integration test scenario:
    Given: All events serialized to an NDJSON file and copied to the staging directory
    When: The file is copied to the loading directory and the pipeline is run with all files globbed, twice
    Then: 
    - Assert the count of loaded events
    - Assert one load_at value
    - Assert that load_at after the second run is the same as load_at after the first run
    - Assert one loaded filename
    """
    # Extract data from the strategy
    events = strategy['events']
    data_dir = strategy['data_dir']
    loading_dir = strategy['loading_dir']
    staging_paths = strategy['staging_paths']
    db_path = os.path.join(data_dir, 'podcast_analytics.duckdb')
    
    try:
        # Copy the file from staging to loading directory
        shutil.copy2(staging_paths[0], loading_dir)
        
        # Prepare dbt variables
        dbt_vars = {
            "data_load_path": loading_dir,
            "test_db_path": db_path
        }
        
        # Act: Run the dbt model first time
        run_dbt_model("raw_events", dbt_vars)
        
        # Capture load_at timestamp after first run
        load_at_query = "SELECT DISTINCT load_at FROM main_bronze.raw_events"
        first_run_load_at = query_database(load_at_query, db_path)
        assert len(first_run_load_at) == 1, f"Expected exactly 1 unique load_at timestamp after first run, got {len(first_run_load_at)}"
        first_load_at_value = first_run_load_at[0][0]
        
        # Act: Run the dbt model second time
        run_dbt_model("raw_events", dbt_vars)  # Second run
        
        # Assert: Verify the results
        
        # 1. Count of loaded events
        count_query = "SELECT COUNT(*) FROM main_bronze.raw_events"
        count_result = query_database(count_query, db_path)
        assert count_result[0][0] == len(events), f"Expected {len(events)} events after running twice, got {count_result[0][0]} (possible duplicates)"
        
        # 2. One load_at value
        load_at_result = query_database(load_at_query, db_path)
        assert len(load_at_result) == 1, f"Expected exactly 1 unique load_at timestamp, got {len(load_at_result)}"
        
        # 3. Assert that load_at after the second run is the same as load_at after the first run
        second_load_at_value = load_at_result[0][0]
        assert first_load_at_value == second_load_at_value, f"Expected load_at to remain the same after second run, got {first_load_at_value} vs {second_load_at_value}"
        
        # 4. One loaded filename
        filename_query = "SELECT DISTINCT filename FROM main_bronze.raw_events"
        filename_result = query_database(filename_query, db_path)
        assert len(filename_result) == 1, f"Expected exactly 1 unique filename, got {len(filename_result)}"
        # assert loading_file_path == filename_result[0][0], f"Expected filename to be {loading_file_path}, got {filename_result[0][0]}"
        
    finally:
        # Clean up the temporary directory created for this example
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)


@given(strategy_with_files(num_files=2))
@settings(max_examples=3, deadline=None)
def test_two_files_load(strategy):
    """
    Test the third integration test scenario:
    Given: All events serialized to two separate NDJSON files and copied to the staging directory
    When: The first file is copied to the loading directory and the pipeline is run once with all files globbed, 
          then the second file is copied to the loading directory and the pipeline run again
    Then: 
    - Assert the count of loaded events equals the sum of event counts
    - Assert two load_at values
    - Assert two filename values
    """
    # Extract data from the strategy
    events = strategy['events']
    data_dir = strategy['data_dir']
    loading_dir = strategy['loading_dir']
    staging_paths = strategy['staging_paths']
    db_path = os.path.join(data_dir, 'podcast_analytics.duckdb')
    
    try:
        # Act: Copy first file from staging to loading and run pipeline
        shutil.copy2(staging_paths[0], loading_dir)
        
        dbt_vars = {
            "data_load_path": loading_dir,
            "test_db_path": db_path
        }
        run_dbt_model("raw_events", dbt_vars)
        
        # Capture results after first run
        count_query = "SELECT COUNT(*) FROM main_bronze.raw_events"
        first_count_result = query_database(count_query, db_path)
        
        load_at_query = "SELECT DISTINCT load_at FROM main_bronze.raw_events"
        first_load_at_result = query_database(load_at_query, db_path)
        
        filename_query = "SELECT DISTINCT filename FROM main_bronze.raw_events"
        first_filename_result = query_database(filename_query, db_path)
        
        # Verify first run loaded only first file's events
        assert len(first_load_at_result) == 1, f"Expected exactly 1 load_at after first run, got {len(first_load_at_result)}"
        assert len(first_filename_result) == 1, f"Expected exactly 1 filename after first run, got {len(first_filename_result)}"
        
        # Act: Copy second file from staging to loading and run pipeline again
        shutil.copy2(staging_paths[1], loading_dir)
        
        run_dbt_model("raw_events", dbt_vars)  # Run with same glob pattern
        
        # Assert: Verify final results
        
        # 1. Count of loaded events equals the sum of event counts
        final_count_result = query_database(count_query, db_path)
        expected_total = len(events)
        assert final_count_result[0][0] == expected_total, f"Expected {expected_total} total events from both files, got {final_count_result[0][0]}"
        
        # 2. Assert two load_at values
        final_load_at_result = query_database(load_at_query, db_path)
        assert len(final_load_at_result) == 2, f"Expected exactly 2 distinct load_at timestamps, got {len(final_load_at_result)}"
        
        # 3. Assert two filename values
        final_filename_result = query_database(filename_query, db_path)
        assert len(final_filename_result) == 2, f"Expected exactly 2 distinct filenames, got {len(final_filename_result)}"
        
    finally:
        # Clean up the temporary directory created for this example
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
