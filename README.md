# TICKIT Enterprise Data Warehouse 🎫

A production-grade data warehouse built on AWS, processing **52.5 million records** from a ticket resale marketplace (similar to Ticketmaster).

## 🏗️ Architecture

```
Bronze (S3 CSV)  →  Silver (S3 Parquet)  →  Gold (Redshift Star Schema)
     ↓                      ↓                          ↓
  Raw CSVs          ETL Cleaned Data          Analytics-Ready DW
```

### Full Pipeline Flow
```
S3 .done file upload
        ↓
  Lambda (S3 Trigger)
        ↓
  Step Functions
        ↓
┌─────────────────────────────────────┐
│   Parallel ETL (6 jobs, ~2 min)     │
│  Lambda: category, venue            │
│  Glue Shell: users, events          │
│  Glue Spark: listings, sales        │
└─────────────────────────────────────┘
        ↓
  sp_load_staging (COPY → Redshift)
        ↓
  sp_load_dimensions (SCD Type 2)
        ↓
  sp_load_facts (Surrogate Key Lookup)
        ↓
    ✅ Pipeline Complete
```

## 📊 Dataset

| Table | Records | File Size | Processing |
|-------|---------|-----------|------------|
| Categories | 11 | 487 B | Lambda |
| Venues | 202 | 9.1 KB | Lambda |
| Users | 2M | 282 MB | Glue Python Shell |
| Events | 3M | 176 MB | Glue Python Shell |
| Listings | 25M | 1.7 GB | Glue Spark |
| Sales | 22.5M | 1.8 GB | Glue Spark |
| **Total** | **~52.5M** | **~4 GB** | |

## 🎯 Business Requirements Answered

| Question | Answer |
|----------|--------|
| Which venues make the most money? | Top: Jacobs Inc Arena, Jacksonville — $82.5M |
| Who are our best customers? | Top: Matthew Kim — $30,043 spent |
| What events sell best? | CONCERTS/Pop — $1.48B total revenue |
| Historical tracking? | SCD Type 2 on dim.users & dim.event |

## 🏛️ Star Schema

```
                    dim.date
                       │
dim.users (seller) ────┤
                       │
dim.users (buyer)  ────┼──── fact.sales ──── dim.event (denormalized)
                       │         │
                       │    fact.listing
                       │
                    dim.users (seller)
```

### Physical Design Decisions

| Table | DISTKEY | SORTKEY | Reason |
|-------|---------|---------|--------|
| fact.sales | eventid | dateid, eventid | Co-locate with dim.event (hottest join) |
| fact.listing | eventid | dateid, eventid | Same pattern |
| dim.users | userid | userid, effective_date | SCD2 point-in-time lookup |
| dim.event | eventid | eventid, effective_date | SCD2 + event join |
| dim.date | ALL | dateid | Tiny table, replicate everywhere |
| stage.* | EVEN | — | Parallel COPY throughput |

## 💰 Cost Optimization

| Strategy | Savings |
|----------|---------|
| Lambda for <1K rows (vs Glue) | ~99% cheaper |
| Python Shell for 1K-10M rows (vs Spark) | ~60% cheaper |
| Spark only for >10M rows | Right-sized compute |
| Parallel execution (6 jobs) | 15 min → 3 min pipeline time |
| Parquet compression | 80% storage reduction vs CSV |

**Overall: 60-90% cost reduction vs all-Spark approach**

## 🔄 SCD Type 2

Tracks historical changes in `dim.users` and `dim.event`:

```sql
-- Old version (when user moves city)
userid=1, city='Jacksonland', is_current=False, expiry_date='2026-09-12'

-- New version
userid=1, city='New York',    is_current=True,  expiry_date='9999-12-31'
```

Historical sales stay linked to the OLD user version via surrogate key — enabling point-in-time reporting.

## 📁 Repository Structure

```
tickit-dw/
├── README.md
├── .gitignore
│
├── sql/                          # Redshift DDL & Stored Procedures
│   ├── 01_create_schemas.sql     # stage, dim, fact schemas
│   ├── 02_stage_tables.sql       # 6 staging tables (DISTSTYLE EVEN)
│   ├── 03_dim_tables.sql         # dim.date, dim.users, dim.event (SCD2)
│   ├── 04_fact_tables.sql        # fact.sales, fact.listing
│   ├── 05_load_dim_date.sql      # Calendar table population
│   └── 06_stored_procedures.sql  # sp_load_staging, sp_load_dimensions, sp_load_facts
│
├── lambda/                       # Lambda ETL functions
│   ├── category_etl.py           # Bronze CSV → Silver Parquet (11 rows)
│   ├── venue_etl.py              # Bronze CSV → Silver Parquet (202 rows)
│   └── s3_trigger.py             # .done file → triggers Step Functions
│
├── glue/                         # AWS Glue ETL scripts
│   ├── users_etl.py              # Python Shell - 2M rows (1 DPU)
│   ├── events_etl.py             # Python Shell - 3M rows, denormalized (1 DPU)
│   ├── listings_etl.py           # Spark - 25M rows (10 × G.2X workers)
│   └── sales_etl.py              # Spark - 22.5M rows (10 × G.2X workers)
│
├── step-functions/
│   └── tickit_pipeline.json      # State machine definition
│
└── docs/
    └── architecture.md           # Detailed architecture decisions
```

## 🚀 Setup & Deployment

### Prerequisites
- AWS CLI configured with appropriate permissions
- Python 3.9+
- AWS account with Redshift Serverless, Glue, Lambda, Step Functions access

### Step 1: Infrastructure
```bash
# Create S3 bucket (replace ACCOUNT_ID)
aws s3 mb s3://ent-dw-ticket-sales-ACCOUNT_ID --region ap-south-1

# Create IAM roles (see docs/architecture.md for policy details)
# - RedshiftServerlessRole
# - TickitGlueRole
# - TickitLambdaRole
# - TickitStepFunctionsRole

# Create Redshift Serverless
aws redshift-serverless create-namespace --namespace-name ent-dw-1 \
    --admin-username admin --admin-user-password <password> \
    --db-name tickit \
    --default-iam-role-arn arn:aws:iam::ACCOUNT_ID:role/RedshiftServerlessRole

aws redshift-serverless create-workgroup --workgroup-name ent-dw-1 \
    --namespace-name ent-dw-1 --base-capacity 8
```

### Step 2: Deploy DDL
```bash
# Run SQL scripts in order via Redshift Data API or Query Editor
# 01_create_schemas.sql → 02_stage_tables.sql → 03_dim_tables.sql
# → 04_fact_tables.sql → 06_stored_procedures.sql
```

### Step 3: Deploy ETL Jobs
```bash
# Upload Glue scripts
aws s3 sync glue/ s3://ent-dw-ticket-sales-ACCOUNT_ID/glue-scripts/

# Deploy Lambda functions
cd lambda
zip category_etl.zip category_etl.py
zip venue_etl.zip venue_etl.py
zip s3_trigger.zip s3_trigger.py

aws lambda create-function --function-name tickit-category-etl \
    --runtime python3.12 --handler category_etl.lambda_handler \
    --role arn:aws:iam::ACCOUNT_ID:role/TickitLambdaRole \
    --zip-file fileb://category_etl.zip

# Add pandas layer
aws lambda update-function-configuration \
    --function-name tickit-category-etl \
    --layers arn:aws:lambda:ap-south-1:336392948345:layer:AWSSDKPandas-Python312:16
```

### Step 4: Create Step Functions
```bash
aws stepfunctions create-state-machine \
    --name tickit-nightly-pipeline \
    --type STANDARD \
    --role-arn arn:aws:iam::ACCOUNT_ID:role/TickitStepFunctionsRole \
    --definition file://step-functions/tickit_pipeline.json
```

### Step 5: Configure S3 Trigger
```bash
# Grant S3 permission to invoke Lambda
aws lambda add-permission --function-name tickit-s3-trigger \
    --statement-id s3-tickit-trigger \
    --action lambda:InvokeFunction \
    --principal s3.amazonaws.com \
    --source-arn arn:aws:s3:::ent-dw-ticket-sales-ACCOUNT_ID

# Configure S3 notification (see docs for full config)
```

### Step 6: Trigger Pipeline
```bash
# Upload source data to bronze
aws s3 cp data/ s3://ent-dw-ticket-sales-ACCOUNT_ID/bronze/ --recursive

# Drop .done file to start pipeline
echo "delivery complete" | aws s3 cp - s3://ent-dw-ticket-sales-ACCOUNT_ID/bronze/.done
```

## 🔧 Real-World Issues Solved

| Issue | Root Cause | Solution |
|-------|-----------|----------|
| Lambda timeout | 15-min limit exceeded on large data | Switched to Glue Python Shell |
| Glue OOM (exit code 37) | 0.0625 DPU (512MB) couldn't handle 282MB CSV | Increased to 1 DPU (4GB) |
| Cross-region COPY error | S3 bucket (us-east-1) ≠ Redshift (ap-south-1) | Added `REGION 'us-east-1'` to COPY |
| CSV header row error | `names=` param + header=True conflict | Used auto-detection with `dtype=str` |
| Redshift DEFAULT params | Stored procedures don't support DEFAULT | Made all params required |
| Spike day failures | Fixed workers couldn't handle 10x volume | G.2X × 10 workers + dynamic scaling |

## 📈 Performance Results

| Stage | Data | Time |
|-------|------|------|
| Lambda: category | 11 rows | 2.9s |
| Lambda: venue | 202 rows | 3.0s |
| Glue Shell: users | 2M rows | 78s |
| Glue Shell: events | 3M rows | 57s |
| Glue Spark: listings | 25M rows | 119s |
| Glue Spark: sales | 22.5M rows | 125s |
| Redshift COPY (all 6) | 52.5M rows | 31s |
| SCD Type 2 (dim load) | 5M rows | 21s |
| Fact load | 47.5M rows | 31s |
| **Total Pipeline** | **52.5M rows** | **~15 min** |

## 🛠️ Tech Stack

- **Storage:** Amazon S3 (Bronze/Silver layers)
- **Compute:** AWS Lambda, AWS Glue (Python Shell + Spark)
- **Warehouse:** Amazon Redshift Serverless
- **Orchestration:** AWS Step Functions
- **Trigger:** S3 Event Notifications → Lambda
- **Languages:** Python 3.12, PySpark, SQL (Redshift)
- **Libraries:** Pandas, PyArrow, boto3

## 👤 Author

Rahul Singh Rana
