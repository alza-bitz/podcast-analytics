# Analysis Questions

## Question 1: Top 10 Most Completed Episodes

### Overview
This analysis answers the question: **"What are the top 10 most completed episodes in the past 7 days?"**

### Usage

#### Default Analysis (using 2024-01-07 as end date)
```bash
dbt run --select question_1_top_completed_episodes
```

#### Custom Date Analysis
```bash
# Analyze completions in the 7 days ending 2024-01-05
dbt run --select question_1_top_completed_episodes --vars '{"analysis_end_date": "2024-01-05"}'
```

#### Query Results
```sql
SELECT * FROM main_analytics.question_1_top_completed_episodes;
```

### Parameters
- `analysis_end_date`: The end date for the 7-day analysis window (defaults to '2024-01-07')
- Analysis covers the period: `[analysis_end_date - 7 days, analysis_end_date)`

### Output Schema
- `episode_id`: Unique identifier for the episode
- `title`: Episode title
- `podcast_id`: Podcast identifier
- `completion_count`: Number of times the episode was completed in the analysis period
- `release_date`: When the episode was originally released
- `duration_seconds`: Episode duration in seconds

### Data Sources
- `fact_user_interactions`: User interaction events (filtered for 'complete' events)
- `dim_episodes`: Episode metadata

## Question 2: Average Listen-Through Rate by Country

### Overview
This analysis answers the question: **"What is the average listen-through rate (completion duration/episode duration) by country?"**

The listen-through rate indicates how much of an episode users actually listen to when they complete it. A rate of 1.0 means users listen to the exact episode duration, while rates above 1.0 might indicate replaying portions or paused time being included.

### Usage

#### Run the Analysis
```bash
dbt run --select question_2_avg_listen_through_rate_by_country
```

#### Query Results
```sql
SELECT * FROM main_analytics.question_2_avg_listen_through_rate_by_country
ORDER BY avg_listen_through_rate DESC;
```

### Output Schema
- `country`: Country where users are located
- `avg_listen_through_rate`: Average ratio of completion duration to episode duration for the country
- `total_completions`: Total number of completion events used in the calculation

### Calculation Logic
- **Listen-through rate** = `completion_duration / episode_duration_seconds`
- Only includes 'complete' events with valid duration data
- Filters out records where episode duration is zero or completion duration is zero/null
- Groups by user country and calculates the average rate

### Data Sources
- `fact_user_interactions`: User interaction events (filtered for 'complete' events)
- `dim_users`: User metadata (for country information)
- `dim_episodes`: Episode metadata (for episode duration)

## Future Questions

As additional analysis questions are implemented (Question 3: Multi-episode listening patterns), their documentation will be added to this file.
