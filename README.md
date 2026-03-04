# ✈️ Flight & Weather Data Engineering Pipeline

A production-style data engineering pipeline that ingests aviation and weather data, stores raw data in S3, transforms it in Snowflake using a layered architecture (Bronze → Silver → Gold), and orchestrates everything with Airflow running in Docker on AWS EC2.

This project simulates a real-world analytics pipeline used to analyze flight delays and cancellations in relation to weather conditions.

---

# 🏗 Architecture

### High-Level Flow

```
External APIs (Flight + Weather)
        │
        ▼
Airflow (Docker on EC2)
        │
        ├── Ingest API data
        ├── Write raw files to S3
        ├── Load to Snowflake (Bronze)
        └── Trigger dbt transformations
                │
                ▼
            Snowflake
        Bronze → Silver → Gold
```

---

# 🛠 Tech Stack

* **Orchestration:** Apache Airflow
* **Compute:** AWS EC2
* **Containerization:** Docker Compose
* **Raw Storage:** AWS S3
* **Data Warehouse:** Snowflake
* **Transformations:** dbt
* **Authentication:** Snowflake RSA keypair authentication

---

# 📂 Repository Structure

```
Flight-and-Weather-DE/
│
├── airflow/
│   └── dags/                  # Airflow DAG definitions
│
├── docker/
│   └── docker-compose.yaml    # Service definitions
│
├── ingestion/                 # API ingestion + S3 write logic
│
├── dbt/
│   └── models/                # Bronze, Silver, Gold transformations
│
├── deploy.sh                  # EC2 deployment script
├── .env.example               # Required environment variables template
└── README.md
```

---

# 🔄 Pipeline Design

## 1️⃣ Ingestion Layer (Airflow)

Airflow runs on EC2 inside Docker and:

* Calls external flight and weather APIs
* Normalizes responses
* Adds ingestion timestamps
* Writes raw files to S3 (partitioned)
* Executes Snowflake load operations
* Triggers dbt models

The DAG is idempotent and designed for scheduled execution.

---

## 2️⃣ Raw Data Layer (S3)

All API responses are stored in S3 before warehouse loading.

Why:

* Decouples ingestion from transformation
* Enables reprocessing
* Provides an auditable raw source of truth
* Supports schema evolution

---

## 3️⃣ Snowflake Layered Architecture

### 🟤 Bronze

* Structured loads from S3
* Minimal transformation
* Preserves ingestion metadata

### 🟡 Silver

* Data cleaning and normalization
* Timestamp standardization
* Deduplication logic
* Status normalization

### 🟢 Gold

* Analytics-ready models
* Flight-level features
* Weather joins
* Delay and cancellation indicators

Gold models are built using dbt.

---

# 🔐 Security & Secrets Management

* No secrets are stored in the repository.
* Snowflake uses RSA keypair authentication.
* Private keys are injected at runtime via environment variables.
* Airflow webserver secret key is externally configured.
* `.env` is excluded from version control.

---

# 🚀 Deployment

## Environment Setup

Copy and configure:

```
cp .env.example .env
```

Populate required variables:

* `AIRFLOW__WEBSERVER__SECRET_KEY`
* `SNOWFLAKE_PRIVATE_KEY_P8_B64`
* `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`
* Other required credentials

## Start Services

```
docker compose --env-file ./.env -f docker/docker-compose.yaml up -d
```

## Deploy Updates (EC2)

```
./deploy.sh
```

---

# 📊 Analytical Use Cases

This pipeline enables:

* Flight delay analysis by airport
* Cancellation trend monitoring
* Weather impact modeling
* Feature engineering for predictive modeling
* Snapshot-based flight status tracking

---

# 🧠 Engineering Decisions

* **S3 before Snowflake** to separate ingestion from warehousing.
* **Layered Snowflake models** for clarity and maintainability.
* **dbt for transformations** to enable modular SQL and testing.
* **Dockerized Airflow** for reproducible deployment.
* **Keypair authentication** for secure Snowflake access.
* **Force-push history sanitization** to eliminate leaked secrets.

---

# 🔮 Potential Enhancements

* Add dbt tests and documentation generation
* Implement monitoring & alerting
* Add CI/CD pipeline (GitHub Actions)
* Introduce data freshness checks
* Add a predictive delay model

---

# 👤 Author

Tom Petrangelo
Data Engineering Portfolio Project

---
