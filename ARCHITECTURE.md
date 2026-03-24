# Architecture Diagram

This architecture covers Airflow orchestration, GCP infrastructure, DBT transformations, and Spark processing:

```mermaid
flowchart LR
    A[Kaggle Dataset: drexibiza/grocery-sales-dataset] -->|download| B[Airflow DAG]
    B --> C[Google Cloud Storage (raw bucket)]
    C --> D[BigQuery (grocery_raw)]
    D --> E[DBT (staging -> marts) in BigQuery (grocery_analytics)]
    D --> F[Spark job (segmentation + recommendations)]
    F --> G[BigQuery (customer_segments + customer_recommendations)]

    B --> H[Secret Manager (kaggle-api-token, GCP creds)]
    B --> I[Terraform infrastructure provisioning]  

    subgraph Local Dev
      J[docker-compose: airflow, postgres, redis, spark] 
      J -- runs --> B
      J -- contains --> E
      J -- contains --> F
    end

    subgraph GCP
      C
      D
      E
      F
      G
      H
      I
    end
```

## Components

- Airflow: orchestrates tasks
- GCS: raw data staging
- BigQuery: primary analytical store
- DBT: SQL transformations
- Spark: ML segmentation and recommendation
- Terraform: infra brokering
- Secret Manager: credentials
