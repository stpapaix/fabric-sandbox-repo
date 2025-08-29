# Data Lineage and Flow Visualization

## Pipeline Data Flow

```mermaid
graph TD
    %% Source Systems
    SQL[(Azure SQL Database)]
    BLOB[(Blob Storage)]
    API[(REST API)]
    
    %% Bronze Layer
    BRONZE[Bronze Layer<br/>Raw Data]
    
    %% Silver Layer  
    SILVER[Silver Layer<br/>Transformed Data]
    
    %% Gold Layer
    GOLD[Gold Layer<br/>Aggregated Metrics]
    
    %% Analytics
    REPORTS[Reports & Dashboards]
    
    %% Notebooks
    NB1[01_data_ingestion.ipynb]
    NB2[02_data_transformation.ipynb] 
    NB3[03_data_aggregation.ipynb]
    
    %% Data Flow
    SQL --> NB1
    BLOB --> NB1
    API --> NB1
    NB1 --> BRONZE
    
    BRONZE --> NB2
    NB2 --> SILVER
    
    SILVER --> NB3
    NB3 --> GOLD
    
    GOLD --> REPORTS
    
    %% Styling
    classDef source fill:#e1f5fe
    classDef bronze fill:#fff3e0
    classDef silver fill:#f3e5f5
    classDef gold fill:#fff8e1
    classDef notebook fill:#e8f5e8
    classDef analytics fill:#fce4ec
    
    class SQL,BLOB,API source
    class BRONZE bronze
    class SILVER silver
    class GOLD gold
    class NB1,NB2,NB3 notebook
    class REPORTS analytics
```

## Detailed Data Transformations

### Bronze → Silver Transformations

```mermaid
graph LR
    %% Bronze Input
    B1[customer_id]
    B2[transaction_amount]
    B3[transaction_date]
    B4[source_system]
    
    %% Transformation Functions
    T1[Categorize Amount]
    T2[Extract Temporal Features]
    T3[Flag High Value]
    T4[Enrich Customer Data]
    
    %% Silver Output
    S1[transaction_category]
    S2[transaction_year/month]
    S3[is_high_value]
    S4[customer_segment]
    S5[customer_region]
    
    %% Connections
    B2 --> T1 --> S1
    B3 --> T2 --> S2
    B2 --> T3 --> S3
    B1 --> T4 --> S4
    B1 --> T4 --> S5
    
    classDef bronze fill:#fff3e0
    classDef transform fill:#e8f5e8
    classDef silver fill:#f3e5f5
    
    class B1,B2,B3,B4 bronze
    class T1,T2,T3,T4 transform
    class S1,S2,S3,S4,S5 silver
```

### Silver → Gold Aggregations

```mermaid
graph LR
    %% Silver Input
    SIL[Silver Transaction Data]
    
    %% Aggregation Types
    AGG1[Daily Aggregation]
    AGG2[Customer Segment Aggregation]
    AGG3[Monthly Trend Aggregation]
    
    %% Gold Output
    G1[Daily Summary Table]
    G2[Segment Analysis Table]
    G3[Monthly Trends Table]
    
    %% Connections
    SIL --> AGG1 --> G1
    SIL --> AGG2 --> G2
    SIL --> AGG3 --> G3
    
    classDef silver fill:#f3e5f5
    classDef agg fill:#e8f5e8
    classDef gold fill:#fff8e1
    
    class SIL silver
    class AGG1,AGG2,AGG3 agg
    class G1,G2,G3 gold
```

## Execution Timeline

```mermaid
gantt
    title MS Fabric Pipeline Daily Execution Schedule
    dateFormat HH:mm
    axisFormat %H:%M
    
    section Data Ingestion
    01_data_ingestion.ipynb    :active, ingestion, 02:00, 60m
    
    section Data Transformation  
    02_data_transformation.ipynb :transformation, after ingestion, 90m
    
    section Data Aggregation
    03_data_aggregation.ipynb   :aggregation, after transformation, 45m
    
    section Quality Checks
    Data Validation            :validation, after aggregation, 15m
```

## Table Dependencies

```mermaid
graph TD
    %% Source Tables
    ST1[customer_transactions<br/>Azure SQL]
    ST2[customer_master<br/>Azure SQL]
    ST3[transaction_files<br/>Blob Storage]
    
    %% Bronze Tables
    BT1[bronze_customer_transactions]
    
    %% Silver Tables
    SLT1[silver_customer_transactions]
    
    %% Gold Tables
    GT1[gold_daily_transaction_summary]
    GT2[gold_customer_segment_analysis]
    GT3[gold_monthly_transaction_trends]
    
    %% Dependencies
    ST1 --> BT1
    ST2 --> BT1
    ST3 --> BT1
    
    BT1 --> SLT1
    
    SLT1 --> GT1
    SLT1 --> GT2
    SLT1 --> GT3
    
    classDef source fill:#e1f5fe
    classDef bronze fill:#fff3e0
    classDef silver fill:#f3e5f5
    classDef gold fill:#fff8e1
    
    class ST1,ST2,ST3 source
    class BT1 bronze
    class SLT1 silver
    class GT1,GT2,GT3 gold
```