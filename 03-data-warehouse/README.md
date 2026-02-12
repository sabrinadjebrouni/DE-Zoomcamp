
# Module 3 Homework: Data Warehousing & BigQuery

Link to the [Homework-03](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/03-data-warehouse/homework.md)

## BigQuery Setup

```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-rides-485210.zoomcamp.external_yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ny-taxi-rides-485210-bucket/yellow_tripdata_2024-*.parquet']
);

-- Check yellow trip data
SELECT * FROM ny-taxi-rides-485210.zoomcamp.external_yellow_tripdata_2024 limit 10;

-- Create a non partitioned/ non clustered table from external table
CREATE OR REPLACE TABLE ny-taxi-rides-485210.zoomcamp.yellow_tripdata_2024_non_partitioned AS
SELECT * FROM ny-taxi-rides-485210.zoomcamp.external_yellow_tripdata_2024;

```

## Question 1. Counting records

**Answer** : 20,332,093 rows

```sql
-- Count yellow trip data total records
SELECT COUNT(*) FROM ny-taxi-rides-485210.zoomcamp.yellow_tripdata_2024_non_partitioned;

```

## Question 2. Data read estimation

**Answer** : 0 MB for the External Table and 155.12 MB for the Materialized Table

```sql

-- Count the distinct number of PULocationIDs for the External Table
SELECT COUNT(PULocationID) FROM ny-taxi-rides-485210.zoomcamp.external_yellow_tripdata_2024;

-- Count the distinct number of PULocationIDs for the Internal Table
SELECT COUNT(PULocationID) FROM ny-taxi-rides-485210.zoomcamp.yellow_tripdata_2024_non_partitioned;

```

## Question 3. Understanding columnar storage

**Answer** : BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

```sql

-- Retrieve the PULocationID
SELECT PULocationID FROM ny-taxi-rides-485210.zoomcamp.yellow_tripdata_2024_non_partitioned;

-- Retrieve the PULocationID and DOLocationID
SELECT PULocationID, DOLocationID FROM ny-taxi-rides-485210.zoomcamp.yellow_tripdata_2024_non_partitioned;

```

## Question 4. Counting zero fare trips

**Answer** : 8,333

```sql
-- How many records have a fare_amount of 0 ?
SELECT COUNT(*) FROM ny-taxi-rides-485210.zoomcamp.yellow_tripdata_2024_non_partitioned WHERE fare_amount=0;

```

## Question 5. Partitioning and clustering

**Answer** : Partition by tpep_dropoff_datetime and Cluster on VendorID

```sql
-- Partition by tpep_dropoff_datetime and Cluster on VendorID
CREATE OR REPLACE TABLE ny-taxi-rides-485210.zoomcamp.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
  SELECT * FROM ny-taxi-rides-485210.zoomcamp.external_yellow_tripdata_2024

```

## Question 6. Partition benefits

**Answer** : 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

```sql
-- Retrieve the distinct VendorIDs from non partitionned table
SELECT DISTINCT(VendorID)
FROM ny-taxi-rides-485210.zoomcamp.yellow_tripdata_2024_non_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Retrieve the distinct VendorIDs from partitionned table
SELECT DISTINCT(VendorID)
FROM ny-taxi-rides-485210.zoomcamp.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

```

## Question 7. External table storage

**Answer** : GCP Bucket

## Question 8. Clustering best practices

**Answer** : False 

Not ALWAYS and here's why :

- **Table Size:** For small tables (typically under 1 GB), the overhead of metadata management for clusters actually outweighs the performance gains. BigQuery is so fast that it can scan a small table entirely in the time it would take to calculate which clusters to skip.

- **Data Frequent Changes:** If your table is subject to constant, massive updates or deletes, the background process that re-clusters the data can struggle to keep up, or you may not see the full benefit of the optimization.

- **Query Patterns:** If you cluster by a column (like VendorID) but your team always queries by a different column (like Store_Location), the clustering provides zero benefit. You only get a performance boost if you query using the specific columns you clustered on.

- **Cost of Storage:** While negligible for most, clustering can slightly increase the metadata storage requirements.


## Question 9. Understanding table scans

**Answer** : OB because BigQuery get its answers from the table Metadata, we can see the number of rows under the Storage Info section in the Details views of the table. Note that this work only for internal tables.
