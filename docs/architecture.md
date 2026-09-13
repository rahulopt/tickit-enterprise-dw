# Architecture Deep Dive

## Why These Design Decisions?

### 1. Cost-Optimized Job Selection

The most important architecture decision in this project.

**Lambda** (`<1K rows`):
- category (11 rows) and venue (202 rows)
- Execution time: ~3 seconds
- Cost: ~$0.000002 per run (essentially free)
- Why not Glue? Glue has minimum 1-minute billing. Lambda is sub-second.

**Glue Python Shell** (`1K–10M rows`):
- users (2M rows, 282MB) and events (3M rows, 176MB)
- 1 DPU = 4GB RAM = handles 282MB CSV comfortably
- Cost: $0.44/hour × (78s/3600) = ~$0.01 per run
- Why not Spark? No cluster overhead, starts in seconds not minutes.
- Real issue: At 0.0625 DPU (default 512MB), users job OOM'd with exit code 37.

**Glue Spark** (`>10M rows`):
- listings (25M, 1.7GB) and sales (22.5M, 1.8GB)
- 10 workers × G.2X (8GB each) = 80GB total memory
- Cost: $0.44 × 10 DPUs × (120s/3600) = ~$0.15 per run
- Why Spark? 1.7GB CSV would OOM any single-node solution.

### 2. Star Schema with Denormalization

**Standard approach** would have separate dim_venue and dim_category tables:
```sql
-- Every query would need:
SELECT ... FROM fact.sales
JOIN dim.event ON ...
JOIN dim.venue ON dim.event.venueid = dim.venue.venueid   -- extra join
JOIN dim.category ON dim.event.catid = dim.category.catid  -- extra join
```

**Our approach** — denormalize venue + category INTO dim.event:
```sql
-- Now every query is:
SELECT ... FROM fact.sales
JOIN dim.event ON ...  -- venue + category columns already here
```

At 22.5M rows in fact.sales, eliminating 2 joins per query = significant performance gain.

### 3. Distribution Keys

**DISTKEY(eventid) on fact.sales and dim.event:**
- Most analytical queries: "revenue by venue" → requires fact.sales JOIN dim.event
- With matching DISTKEY, this join is node-local (no network shuffle)
- Alternative considered: DISTKEY(userid) — rejected because users appear as both seller AND buyer (role-playing dimension), requiring 2 joins vs 1 for eventid

**DISTSTYLE ALL on dim.date:**
- Only 365-1000 rows
- Every fact query joins to date for time filtering
- Cost of replication: negligible
- Benefit: date join is always node-local

**DISTSTYLE EVEN on stage tables:**
- Stage tables are TRUNCATE + COPY every night
- No joins performed on stage tables
- EVEN = maximum COPY parallelism

### 4. SCD Type 2

**Why track history?**
- User moves from NYC to LA
- Old sales should still show "sold in NYC" for accurate historical reporting
- New sales show "sold in LA"

**Surrogate key pattern:**
```
userid=1, user_key=1001, city='NYC', is_current=False  ← old version
userid=1, user_key=1002, city='LA',  is_current=True   ← new version
```

fact.sales stores user_key (not userid), so:
- Sales made when user was in NYC → user_key=1001 → NYC
- Sales made after move → user_key=1002 → LA
- **Historical accuracy preserved**

### 5. Step Functions Polling Pattern

Redshift Data API is **asynchronous** — you submit a query and poll for results.

```
ExecuteStatement → returns ID
     ↓
  Wait (30-180s)
     ↓
DescribeStatement → check Status
     ↓
  Choice: FINISHED → next step
          FAILED   → fail pipeline
          other    → loop back to Wait
```

This avoids persistent connections (no "too many connections" error) and handles long-running queries gracefully.

### 6. Idempotency in Fact Loading

```sql
-- NOT EXISTS check prevents double-loading
INSERT INTO fact.sales (...)
SELECT ... FROM stage.sales s
WHERE NOT EXISTS (
    SELECT 1 FROM fact.sales f WHERE f.salesid = s.salesid
);
```

If Step Functions retries due to transient failure, the second run won't create duplicates.

## IAM Role Design

| Role | Used By | Permissions |
|------|---------|------------|
| RedshiftServerlessRole | Redshift | S3 read (for COPY) |
| TickitGlueRole | Glue jobs | S3 full, CloudWatch, Glue service |
| TickitLambdaRole | Lambda functions | S3 full, Step Functions, Redshift Data API |
| TickitStepFunctionsRole | Step Functions | Lambda invoke, Glue start, Redshift Data API |

## Cross-Region Consideration

S3 bucket is in `us-east-1` (default region), Redshift is in `ap-south-1`.

Redshift COPY requires explicit `REGION` parameter for cross-region:
```sql
COPY stage.users
FROM 's3://bucket/silver/users/'
FORMAT AS PARQUET
IAM_ROLE 'arn:aws:iam::...:role/RedshiftServerlessRole'
REGION 'us-east-1';  -- ← Required for cross-region
```

In production: keep S3 and Redshift in same region to avoid cross-region data transfer costs.
