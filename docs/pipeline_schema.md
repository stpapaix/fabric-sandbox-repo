# MS Fabric Sandbox Data Pipeline Schema

## Overview
This document provides a comprehensive schema explaining the execution flow and components of the MS Fabric Sandbox Data Pipeline, with detailed analysis of each notebook and its operations.

## Pipeline Architecture

### Data Flow Layers
```
Source Systems → Bronze Layer → Silver Layer → Gold Layer → Analytics
     ↓              ↓             ↓             ↓           ↓
Raw Data      Ingested Data  Transformed   Aggregated   Reports
                              Data          Metrics      Dashboards
```

## Execution Schema

### 1. Data Ingestion Pipeline (`01_data_ingestion.ipynb`)

**Purpose**: Extract raw data from source systems and load into Bronze layer

**Execution Steps**:
1. **Source Connection Setup**
   - Establishes connections to Azure SQL Database, Blob Storage, and API endpoints
   - Validates authentication and connectivity
   - Sets up connection pooling for optimal performance

2. **Data Extraction**
   - Executes incremental data pulls using watermark columns (e.g., `created_date >= DATEADD(day, -1, GETDATE())`)
   - Handles large datasets with pagination and chunking
   - Implements error handling and retry logic

3. **Raw Data Validation**
   - Performs basic schema validation
   - Checks for data completeness
   - Logs data quality metrics

4. **Bronze Layer Storage**
   - Saves raw data in Delta format
   - Adds metadata columns: `_ingestion_timestamp`, `_source_file`
   - Partitions data by date for optimal query performance
   - Preserves original data structure and values

**Input Sources**:
- Azure SQL Database: `customer_transactions` table
- Blob Storage: Historical transaction files
- REST API: Real-time transaction feeds

**Output**: 
- Bronze tables: `bronze_customer_transactions`
- Validation reports in JSON format

**Execution Frequency**: Daily at 2:00 AM UTC

---

### 2. Data Transformation Pipeline (`02_data_transformation.ipynb`)

**Purpose**: Apply business rules, cleanse data, and create enriched Silver layer tables

**Execution Steps**:
1. **Bronze Data Loading**
   - Reads latest data from Bronze layer Delta tables
   - Applies incremental processing logic
   - Handles data schema evolution

2. **Business Rule Application**
   - **Transaction Categorization**: Classifies amounts into Small/Medium/Large/Very Large categories
   - **Temporal Attributes**: Extracts year, month, day of week from transaction dates
   - **High-Value Flagging**: Identifies transactions above $750 threshold
   - **Data Type Conversions**: Ensures proper data types for downstream processing

3. **Data Enrichment**
   - Joins with customer master data for segmentation information
   - Calculates customer-level aggregated metrics:
     - Total transaction count per customer
     - Total and average transaction amounts
     - First and last transaction dates
   - Adds derived attributes for analytics

4. **Data Quality Checks**
   - Validates business rule application
   - Checks for referential integrity
   - Monitors data distribution changes

5. **Silver Layer Storage**
   - Saves transformed data in Delta format
   - Partitions by year and month for query optimization
   - Adds transformation metadata: `_transformation_timestamp`, `_transformation_version`

**Input**: Bronze layer tables
**Output**: Silver tables: `silver_customer_transactions` with enriched columns
**Execution Frequency**: Daily at 4:00 AM UTC (after ingestion)

---

### 3. Data Aggregation Pipeline (`03_data_aggregation.ipynb`)

**Purpose**: Create business-ready aggregated tables and metrics for reporting

**Execution Steps**:
1. **Silver Data Loading**
   - Reads transformed data from Silver layer
   - Applies business date filters for relevant time periods

2. **Daily Summary Creation**
   - Aggregates by transaction date:
     - Total transactions, revenue, unique customers
     - Statistical measures: mean, median, standard deviation
     - High-value transaction counts and percentages
     - Average revenue per customer

3. **Customer Segment Analysis**
   - Groups by customer segment and region:
     - Customer counts and transaction volumes
     - Revenue metrics by segment
     - Behavioral patterns (transactions per customer)
     - Geographic distribution analysis

4. **Monthly Trend Analysis**
   - Calculates monthly aggregations:
     - Period-over-period growth rates
     - Seasonal pattern identification
     - Customer acquisition and retention metrics
     - Revenue trend analysis

5. **Gold Layer Storage**
   - Saves aggregated tables in Delta format:
     - `gold_daily_transaction_summary`
     - `gold_customer_segment_analysis`
     - `gold_monthly_transaction_trends`
   - Adds pipeline metadata: `_created_timestamp`, `_pipeline_run_id`

**Input**: Silver layer tables
**Output**: Gold layer aggregated tables ready for reporting
**Execution Frequency**: Daily at 6:00 AM UTC (after transformation)

## Data Schema Details

### Bronze Layer Schema
```sql
bronze_customer_transactions:
- id: BIGINT (Primary Key)
- customer_id: STRING
- transaction_amount: DECIMAL(10,2)
- transaction_date: TIMESTAMP
- source_system: STRING
- _ingestion_timestamp: TIMESTAMP
- _source_file: STRING
```

### Silver Layer Schema
```sql
silver_customer_transactions:
- id: BIGINT
- customer_id: STRING
- transaction_amount: DECIMAL(10,2)
- transaction_date: TIMESTAMP
- transaction_category: STRING (Small/Medium/Large/Very Large)
- transaction_year: INT
- transaction_month: INT
- transaction_day_of_week: STRING
- is_high_value: BOOLEAN
- customer_segment: STRING (Premium/Standard/Basic)
- customer_region: STRING (North/South/East/West)
- customer_age_group: STRING
- total_transactions: INT
- total_amount: DECIMAL(12,2)
- avg_amount: DECIMAL(10,2)
- first_transaction_date: TIMESTAMP
- last_transaction_date: TIMESTAMP
- _transformation_timestamp: TIMESTAMP
- _transformation_version: STRING
```

### Gold Layer Schemas

#### Daily Summary Table
```sql
gold_daily_transaction_summary:
- transaction_date: DATE
- total_transactions: INT
- total_amount: DECIMAL(12,2)
- avg_amount: DECIMAL(10,2)
- median_amount: DECIMAL(10,2)
- std_amount: DECIMAL(10,2)
- unique_customers: INT
- high_value_transactions: INT
- avg_amount_per_customer: DECIMAL(10,2)
- high_value_percentage: DECIMAL(5,2)
- _created_timestamp: TIMESTAMP
- _pipeline_run_id: STRING
```

#### Customer Segment Analysis Table
```sql
gold_customer_segment_analysis:
- customer_segment: STRING
- customer_region: STRING
- unique_customers: INT
- total_transactions: INT
- total_amount: DECIMAL(12,2)
- avg_transaction_amount: DECIMAL(10,2)
- high_value_transactions: INT
- revenue_per_customer: DECIMAL(10,2)
- transactions_per_customer: DECIMAL(6,2)
- _created_timestamp: TIMESTAMP
- _pipeline_run_id: STRING
```

#### Monthly Trends Table
```sql
gold_monthly_transaction_trends:
- year: INT
- month: INT
- month_name: STRING
- total_transactions: INT
- total_amount: DECIMAL(12,2)
- avg_amount: DECIMAL(10,2)
- unique_customers: INT
- high_value_transactions: INT
- revenue_growth_rate: DECIMAL(6,2)
- transaction_growth_rate: DECIMAL(6,2)
- _created_timestamp: TIMESTAMP
- _pipeline_run_id: STRING
```

## Pipeline Dependencies and Orchestration

### Execution Order
1. `01_data_ingestion.ipynb` (2:00 AM) - **Must complete successfully**
2. `02_data_transformation.ipynb` (4:00 AM) - **Depends on Step 1**
3. `03_data_aggregation.ipynb` (6:00 AM) - **Depends on Step 2**

### Error Handling
- Each notebook includes comprehensive error handling and logging
- Failed runs trigger automatic retries (configurable in `pipeline_config.json`)
- Data quality validation reports are generated at each layer
- Alerts are sent to the data team for critical failures

### Monitoring and Observability
- Pipeline execution metrics are tracked and logged
- Data lineage is maintained through metadata columns
- Performance metrics are collected for optimization
- Data quality trends are monitored over time

## Business Value

### Key Metrics Delivered
1. **Operational Metrics**: Daily transaction volumes, revenue trends
2. **Customer Analytics**: Segment performance, behavioral patterns
3. **Financial Analytics**: Revenue growth, high-value transaction tracking
4. **Data Quality Metrics**: Completeness, accuracy, freshness indicators

### Use Cases Enabled
- Executive dashboards and reporting
- Customer segmentation and targeting
- Financial forecasting and planning
- Data quality monitoring and alerting
- Regulatory compliance reporting

## Technical Specifications

### Compute Requirements
- **Small workloads**: 2-4 cores, 8-16 GB RAM
- **Medium workloads**: 4-8 cores, 16-32 GB RAM
- **Auto-termination**: 30 minutes of inactivity

### Storage Optimization
- Delta Lake format for ACID transactions and time travel
- Partitioning by date columns for query performance
- Z-ordering on frequently filtered columns
- Automatic compaction and optimization

### Security and Compliance
- Managed Identity authentication for Azure services
- Data encryption at rest and in transit
- Role-based access control (RBAC)
- Data classification and sensitivity labeling