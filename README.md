# Podcast Analytics Data Pipeline

## Summary

A data pipeline for processing podcast event logs to enable analytics on user engagement and episode performance. The solution addresses the requirements outlined in the [problem statement](.github/instructions/problem_statement_and_requirements.instructions.md) and follows the architecture described in the [solution design](.github/instructions/solution_design.instructions.md).

The pipeline processes raw JSON event logs from podcast streaming interactions, validates and cleanses the data, and transforms it into an analytics-ready star schema to answer key business questions about podcast performance and user behavior.

## Features

### Data Pipeline Architecture
- **[Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)**: Bronze (raw), Silver (cleansed), Gold (analytics) data layers
- **[ELT](https://wikipedia.org/wiki/Extract,_load,_transform) Pattern**: Extract and Load raw data, then Transform using [dbt](https://www.getdbt.com)
- **Incremental Processing**: Handles new data efficiently with incremental models
- **Data Quality Validation**: Comprehensive validation rules with error tracking
- **Star Schema**: Optimized analytics model with fact and dimension tables

### Analytics Capabilities
- **Episode Performance**: Top completed episodes analysis
- **User Engagement**: Listen-through rates by country
- **User Behavior**: Multi-episode listening patterns
- **Temporal Analysis**: Time-based filtering for recent activity

### Data Sources
- **Event Logs**: JSON files containing user interaction events (play, pause, seek, complete)
- **User Reference Data**: CSV with user demographics and signup information
- **Episode Reference Data**: CSV with episode metadata and podcast information

### Supported Databases
- **[DuckDB](https://duckdb.org)**: Primary target for local development and testing
- **[Snowflake](https://www.snowflake.com)**: Production target (configured but not yet implemented)

## Prerequisites

- Python 3.12+ and packages from [requirements.txt](requirements.txt)
- DuckDB
- Git

Alternatively, use an editor or environment that supports [dev containers](https://containers.dev). The supplied [devcontainer.json](.devcontainer/devcontainer.json) will install all the above prerequisites.

## Usage

### Initial Setup
1. Ensure you're in the project root directory
2. Install dependencies (if not using a dev container):

   ```bash
   pip install -r requirements.txt
   ```

### Running the Complete Pipeline
Execute all pipeline steps in order:

1. **Load seed data (reference tables)**:
   ```bash
   dbt seed
   ```

2. **Add some event data**

   ```bash
   mkdir data_split/ data_load/
   split data_example/event_logs.json data_split/event_logs_ --suffix-length=2 --numeric-suffixes=1 --additional-suffix=.json
   cp data_split/event_logs_01.json data_load/
   ```

3. **Add some episode data**

   A script is provided to split the example CSV data files with header retained. These split files can then be copied into the `data_load` directory:

   ```bash
   mkdir data_split/ data_load/
   cd data_example
   ./split_episodes.sh
   cp ../data_split/episodes_*.csv ../data_load/
   cd ..
   ```

2. **Run all models** (raw → validated → cleansed → analytics):
   ```bash
   dbt run
   ```

3. **Run data quality tests**:
   ```bash
   dbt test
   ```

### Running Specific Pipeline Stages

#### Bronze Layer (Raw Data)
```bash
# Load raw event data
dbt run --select raw_events

# Validate raw data
dbt run --select validated_events
```

#### Silver Layer (Cleansed Data)
```bash
# Clean and normalize events
dbt run --select cleansed_events
```

#### Gold Layer (Analytics)
```bash
# Build analytics models
dbt run --select analytics
```

### Running Analysis Questions

For detailed information about running the analysis questions, see the separate [Analysis Questions](doc/analysis-questions.md) document.

### Data Refresh and Incremental Processing

#### Full Refresh (Rebuild All Models)
```bash
dbt run --full-refresh
```

#### Process Only New Data
```bash
dbt run  # Incremental models will automatically process only new data
```

### Adding New Event Data
1. Place new JSON by running `split` as described above and copying the results into the `data_load` directory.

2. Run the pipeline to process new events:

   ```bash
   dbt run --select raw_events+  # Run raw_events and all downstream models
   ```

### Adding New Episode Data

1. Place new CSV files by running `split_episodes.sh` as described above and copying the results into the `data_load` directory.

2. Run the pipeline to process new episodes:

   ```bash
   dbt run --select cleansed_episodes+  # Run cleansed_episodes and all downstream models
   ```


## Development

### Approach

For detailed information about the development approach used, see the separate [Development Approach](doc/development-approach.md) document.

### Integration Tests
The project includes comprehensive integration tests using pytest and hypothesis for data generation:

```bash
# Run all integration tests
python -m pytest integration/ -v

# Run specific test
python -m pytest integration/test_extract_and_load.py -v
```

### Development Commands

#### Compile Models (Check SQL Syntax)
```bash
dbt compile
```

#### Generate Documentation
```bash
dbt docs generate
dbt docs serve
```

#### Check Data Lineage
```bash
dbt ls --select +question_1_top_completed_episodes  # Show upstream dependencies
dbt ls --select question_1_top_completed_episodes+  # Show downstream dependencies
```

## Technical Architecture

For detailed technical information about the data models, quality framework, and architectural decisions, see the separate [Technical Architecture](doc/technical-architecture.md) documentation.

## Acknowledgements

This project was developed as a take-home technical assessment for a Senior Data Engineer position, implementing the requirements specified in the [problem statement](.github/instructions/problem_statement_and_requirements.instructions.md).

## License

The problem statement and requirements are copyright (c) 2025 [global.com](https://global.com).

This project is licensed under the [MIT License](LICENSE).

Copyright (c) 2025 Alex Coyle
