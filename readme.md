# MS Fabric Sandbox Data Pipeline

A comprehensive data pipeline implementation for Microsoft Fabric showcasing modern data engineering patterns with Bronze-Silver-Gold architecture.

## 🏗️ Architecture Overview

This repository contains a complete end-to-end data pipeline demonstrating:
- **Bronze Layer**: Raw data ingestion from multiple sources
- **Silver Layer**: Cleansed and transformed business data
- **Gold Layer**: Aggregated analytics-ready tables

```
Source Systems → Bronze → Silver → Gold → Analytics
     ↓            ↓       ↓       ↓         ↓
   Raw Data   Ingested  Clean   Business   Reports
              Data      Data    Metrics    Dashboards
```

## 📁 Repository Structure

```
fabric-sandbox-repo/
├── notebooks/                      # Jupyter notebooks for data processing
│   ├── 01_data_ingestion.ipynb    # Bronze layer - data ingestion
│   ├── 02_data_transformation.ipynb # Silver layer - data transformation
│   └── 03_data_aggregation.ipynb   # Gold layer - business aggregations
├── config/                         # Configuration files
│   └── pipeline_config.json       # Pipeline configuration settings
├── docs/                           # Documentation
│   └── pipeline_schema.md          # Detailed pipeline schema and execution guide
└── README.md                       # This file
```

## 🚀 Quick Start

### Prerequisites
- Microsoft Fabric workspace access
- Python 3.9+ environment
- Required packages: pandas, pyarrow, delta-spark (see notebooks for imports)

### Pipeline Execution

1. **Data Ingestion** (Bronze Layer)
   ```bash
   # Run the ingestion notebook
   jupyter nbconvert --execute notebooks/01_data_ingestion.ipynb
   ```

2. **Data Transformation** (Silver Layer)
   ```bash
   # Run the transformation notebook
   jupyter nbconvert --execute notebooks/02_data_transformation.ipynb
   ```

3. **Data Aggregation** (Gold Layer)
   ```bash
   # Run the aggregation notebook
   jupyter nbconvert --execute notebooks/03_data_aggregation.ipynb
   ```

## 📊 Data Pipeline Components

### 1. Data Ingestion (`01_data_ingestion.ipynb`)
- **Purpose**: Extract raw data from source systems
- **Sources**: Azure SQL Database, Blob Storage, REST APIs
- **Output**: Bronze layer tables with metadata
- **Schedule**: Daily at 2:00 AM UTC

### 2. Data Transformation (`02_data_transformation.ipynb`)
- **Purpose**: Apply business rules and data enrichment
- **Transformations**: Categorization, temporal attributes, customer enrichment
- **Output**: Silver layer tables with clean, enriched data
- **Schedule**: Daily at 4:00 AM UTC

### 3. Data Aggregation (`03_data_aggregation.ipynb`)
- **Purpose**: Create business metrics and summary tables
- **Aggregations**: Daily summaries, customer segments, monthly trends
- **Output**: Gold layer analytics-ready tables
- **Schedule**: Daily at 6:00 AM UTC

## 📈 Business Metrics Generated

The pipeline produces several key analytical tables:

- **Daily Transaction Summary**: Revenue, volume, and customer metrics by day
- **Customer Segment Analysis**: Performance metrics by customer segment and region
- **Monthly Trends**: Growth rates and seasonal patterns

## 🔧 Configuration

Pipeline configuration is managed through `config/pipeline_config.json`:

```json
{
  "pipeline": {
    "name": "fabric_sandbox_datapipeline",
    "version": "1.0.0"
  },
  "lakehouse": {
    "name": "fabric_sandbox_lakehouse",
    "storage_format": "delta"
  }
}
```

## 📚 Documentation

For detailed information about the pipeline schema, execution flow, and data transformations, see:
- [Pipeline Schema Documentation](docs/pipeline_schema.md)

## 🔒 Security & Compliance

- **Authentication**: Managed Identity for Azure services
- **Encryption**: Data encrypted at rest and in transit  
- **Access Control**: Role-based access control (RBAC)
- **Data Classification**: Internal data classification level

## 🏃‍♂️ Execution Schedule

| Notebook | Time (UTC) | Dependencies | Duration |
|----------|------------|--------------|----------|
| Data Ingestion | 2:00 AM | Source systems | ~60 min |
| Data Transformation | 4:00 AM | Ingestion complete | ~90 min |
| Data Aggregation | 6:00 AM | Transformation complete | ~45 min |

## 🛠️ Monitoring & Alerting

- Pipeline execution metrics tracked
- Data quality validation reports generated
- Automatic retry on failures
- Email alerts for critical issues

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test the pipeline end-to-end
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For questions or issues:
- Create an issue in this repository
- Contact the data engineering team
- Refer to the [Pipeline Schema Documentation](docs/pipeline_schema.md) for detailed technical information
