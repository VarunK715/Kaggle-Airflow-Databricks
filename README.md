# Kaggle-Airflow-Databricks
Ingestion: Automated a Python script to pull data from the Kaggle API directly into Databricks Unity Catalog Volumes.
Orchestration: Managed the entire lifecycle using Apache Airflow, triggering Databricks Jobs to handle task dependencies.
Medallion Architecture:
Bronze: Raw landing zone where data is moved from Volumes with zero modifications to preserve the original state.
Silver: Applied data cleaning by standardizing date formats, casting column types, handling null values, and removing duplicates.
Gold: Modeled data into Fact and Dimension tables. Implemented SCD Type 2 to track historical changes and used Hash Keys for efficient record versioning and deduplication.
Governance: Leveraged Unity Catalog for centralized data access control and lineage tracking across all layers.

Tools Used
Orchestration: Airflow
Compute: Databricks (PySpark/Notebooks)
Storage & Governance: Unity Catalog, Volumes
Architecture: Medallion (Bronze, Silver, Gold), Dimensional Modeling (SCD Type 2)
