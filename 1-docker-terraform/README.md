# Data Engineering Zoomcamp 2026 - Module 1 Homework

This repository contains my answers for the first module of the Data Engineering Zoomcamp, covering Docker, SQL, and Terraform.


### Question 1: Understanding Docker images
**Command executed:**
```bash
docker run -it --rm --entrypoint=bash python:3.13
pip --version
```
**Answer:** 25.3



### Question 2: Understanding Docker networking and docker-compose

In a `docker-compose.yaml` setup, containers communicating with each other use the **Internal Network**. 

- **Hostname:** The service name (`db`) or the `container_name` (`postgres`).
- **Port:** The internal port (`5432`). 

**Answer:** `db:5432` (or `postgres:5432`)



### Question 3: Counting short trips


**SQL Query:**
```sql
SELECT
    COUNT(1) AS short_trips
FROM
    yellow_taxi_data
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
```
**Answer:** 8007



### Question 4: Longest trip for each day


**SQL Query:**
```sql
SELECT
    CAST(lpep_pickup_datetime AS DATE) AS "day",
	MAX(trip_distance) AS "distance"
FROM
    yellow_taxi_data
WHERE 
	trip_distance <100
GROUP BY "day"
ORDER BY "distance" DESC
LIMIT 1;
```
**Answer:** 2025-11-14


### Question 5: Biggest pickup zone

**SQL Query:**
```sql
SELECT
	CAST(lpep_pickup_datetime AS DATE) AS "day",
	z.zone, SUM(total_amount)
FROM 
	yellow_taxi_data ydt,
	zones z
WHERE
	CAST(lpep_pickup_datetime AS DATE) ='2025-11-18' 
	AND ydt.pulocationid = z.locationid
GROUP BY
	"day",z.zone
ORDER BY
	SUM(total_amount) DESC;
```
**Answer:** East Harlem North


### Question 6: Largest tip

**SQL Query:**
```sql
SELECT
	dolocationid, z_do.zone AS "do_zone", MAX(tip_amount) AS "tip"
FROM
	yellow_taxi_data ydt,
	zones z_pu,
	zones z_do
WHERE 
	ydt.pulocationid=z_pu.locationid
	AND ydt.dolocationid=z_do.locationid
	AND z_pu.zone = 'East Harlem North'
	AND lpep_pickup_datetime >= '2025-11-01' 
	AND lpep_pickup_datetime < '2025-12-01'
GROUP BY
	dolocationid, z_do.zone
ORDER BY
	"tip" DESC;
```
**Answer:** Yorkville West



### Question 7: Terraform Workflow

Given this following workflow: 

1. Downloading the provider plugins and setting up backend
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform

**Answer:** `terraform init, terraform apply -auto-approve, terraform destroy`
