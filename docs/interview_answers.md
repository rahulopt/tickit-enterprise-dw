# Interview Q&A — TICKIT Data Warehouse

## The 30-Second Elevator Pitch

> "I built a production data warehouse on Redshift Serverless processing 52.5 million records nightly. The ETL pipeline uses cost-optimized job selection — Lambda for small reference data, Glue Python Shell for medium datasets, Spark for high-volume transactions. The star schema implements SCD Type 2 for historical tracking with denormalized dimensions to minimize joins. Parallel processing in Step Functions reduces pipeline time from 15 minutes to 3 minutes, and the overall cost is 60-90% less than a naive all-Spark approach."

---

## Core Questions

**Q: Why did you use different compute options for different tables?**

> "Cost optimization based on data volume. Lambda for category (11 rows) and venue (202 rows) — sub-second execution, essentially free. Glue Python Shell for users (2M) and events (3M) — Pandas handles this in 4GB RAM on a single node, no cluster spin-up cost. Spark for listings (25M, 1.7GB) and sales (22.5M, 1.8GB) — distributed processing is the only option for data this size. This approach reduced processing costs by 60-90% compared to running everything on Spark."

---

**Q: What is SCD Type 2 and why did you use it?**

> "SCD Type 2 tracks historical changes by keeping multiple versions of a dimension record. When a user changes city from NYC to LA, instead of overwriting the old value, we close the old record (set expiry_date = yesterday, is_current = False) and insert a new record with the new city (effective_date = today, is_current = True). Historical sales stay linked to the old version via surrogate key, so we can accurately report 'revenue from NYC customers' even after those customers moved. Without SCD Type 2, you lose that historical context."

---

**Q: Why did you denormalize venue and category into dim.event?**

> "Analytical queries almost always want event context along with venue and category information. Without denormalization, every query would require 3 joins: fact.sales → dim.event → dim.venue → dim.category. By embedding venue and category columns directly into dim.event, we eliminate 2 joins per query. At 22.5 million rows in fact.sales, this is a significant performance improvement. The tradeoff is some data redundancy in dim.event, but for a read-heavy analytical workload, that's the right call."

---

**Q: How did you handle the Glue job OOM error?**

> "The users ETL job was failing with exit code 37, which is Python's out-of-memory signal. The job was configured at 0.0625 DPU, which gives only 512MB of RAM. Loading a 282MB CSV into Pandas requires roughly 3-4x the file size in memory during processing. I increased the DPU to 1, which gives 4GB of RAM, and the job ran successfully. This is a real-world issue — default Glue Python Shell settings are often too small for production data volumes."

---

**Q: Why use Step Functions instead of a simple script?**

> "Three reasons. First, the Redshift Data API is asynchronous — you submit a query and poll for completion. Step Functions' Wait + DescribeStatement + Choice pattern handles this elegantly without holding a connection open. Second, I needed the 6 ETL jobs to run in parallel, which Step Functions' Parallel state handles natively — reducing pipeline time from ~15 minutes to ~3 minutes. Third, built-in retry logic handles transient failures like Lambda throttling or Glue cluster startup delays."

---

**Q: How do you prevent duplicate data if the pipeline runs twice?**

> "Idempotency in the fact loading stored procedure. The INSERT uses a NOT EXISTS subquery that checks if the salesid already exists in fact.sales. If Step Functions retries due to a transient failure and re-runs sp_load_facts, the second run simply skips all records that were already loaded. The staging tables use TRUNCATE + COPY pattern, so they're always replaced with tonight's batch. Dimensions use the SCD Type 2 NOT EXISTS check — if a record's is_current=True already exists, we don't insert another."

---

**Q: What distribution key did you choose and why?**

> "DISTKEY(eventid) on both fact.sales and dim.event. The most frequent and expensive query pattern is 'revenue by venue', which requires joining fact.sales to dim.event. When both tables are distributed on eventid, the join is node-local — Redshift never has to shuffle data across nodes. I considered DISTKEY(userid) since there are user dimensions too, but users appear twice per sale (seller and buyer), requiring 2 joins, while eventid requires only 1. The compound sort key SORTKEY(dateid, eventid) supports time-range filters which analysts use constantly."

---

**Q: What real-world production issues did you encounter?**

> "Several. The most impactful was the cross-region S3 issue — our S3 bucket was in us-east-1 but Redshift was in ap-south-1. Redshift's COPY command failed with 'PermanentRedirect' until I added the REGION parameter. Another was discovering that Redshift stored procedures don't support DEFAULT parameter values, unlike PostgreSQL — I had to make all parameters required and pass CURRENT_DATE explicitly from Step Functions. And the CSV header row issue — the source team changed their delivery format to include headers, which broke our ETL that was using explicit column name mapping. Always verify your assumptions about source data format."

---

## Positioning by Experience Level

### 3 Years Experience
Focus on: technical execution, what you built, tools used
- "I implemented ETL pipelines using Lambda, Glue Python Shell, and Spark based on data volume"
- "I created a star schema with SCD Type 2 tracking using surrogate keys"
- "I built Step Functions orchestration with parallel execution"

### 5+ Years Experience  
Focus on: design decisions, tradeoffs, business impact
- "I architected a cost-optimized multi-tier processing system with intelligent job routing based on data volume characteristics"
- "I chose to denormalize venue and category into dim.event, eliminating 2 joins per analytical query at 22.5M row fact scale"
- "The parallel Step Functions design reduced pipeline execution time by 80%, from 15 minutes to 3 minutes"
