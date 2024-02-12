# Data Engineering Zoomcamp Course 2024
# Module 3: Data Warehouse

Repo Folder for tasks and homeworks included in the Module 3: Data Engineering Zoomcamp course Cohort 2024.

## Content of the Module

1. Data Warehouse
2. BigQuery
3. Partitioning and clustering
4. BigQuery best practices
5. Internals of BigQuery
6. BigQuery Machine Learning

## Homework
<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>


Code to create external table:
```sql
CREATE EXTERNAL TABLE `banded-pad-411315.ny_taxi.ext_green_taxi_2022`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://mage-zoomcamp-ems/green_taxi_2022/*']
    );

```

To create the table without partitioning or clustering, go to BigQuery Studio, select the dataset and click on `CREATE TABLE`. Then select the source `Google Cloud Storage`, the files (gs://<bucket_name>/green_taxi_2022/*) and set the file format to Parquet.

## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- 840,402
- 1,936,423
- 253,647

**Answer**: 840,402
You can find the number of rows in the Details tab, Storage Info section in the materialized table.

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

**Answer** 0B for the external table and 6.41MB for the Materialized Table

Query materialized table:
```sql
SELECT COUNT(DISTINCT PULocationID) FROM `banded-pad-411315.ny_taxi.green_taxi_2022`
```

Query external table:
```sql
SELECT COUNT(DISTINCT PULocationID) FROM `banded-pad-411315.ny_taxi.ext_green_taxi_2022`
```

## Question 3:
How many records have a fare_amount of 0?
- 12,488
- 128,219
- 112
- 1,622

**Answer**: 1622
```sql
SELECT COUNT(*)
FROM ny_taxi.green_taxi_2022
WHERE fare_amount=0
```

## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime  Cluster on PUlocationID
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

**Answer**
- Partition by lpep_pickup_datetime  Cluster on PUlocationID

```sql
CREATE TABLE ny_taxi.green_taxi_2022_part
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
  AS 
    SELECT *
    FROM `banded-pad-411315.ny_taxi.green_taxi_2022`
```
## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

**Answer**: 12,82 for non-partiotioned and 1,12MB for partitioned

For non-paritioned table:
```sql
SELECT DISTINCT PULocationID
FROM `banded-pad-411315.ny_taxi.green_taxi_2022`
WHERE DATE(lpep_pickup_datetime)>= CAST('2022-06-01' AS DATE)
AND DATE(lpep_pickup_datetime)<= CAST('2022-06-30' AS DATE)
```
For partitioned table:
```sql
SELECT DISTINCT PULocationID
FROM `banded-pad-411315.ny_taxi.green_taxi_2022_part`
WHERE DATE(lpep_pickup_datetime)>= CAST('2022-06-01' AS DATE)
AND DATE(lpep_pickup_datetime)<= CAST('2022-06-30' AS DATE)
```


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Big Table
- Container Registry

**Answer**: In the GCP Bucket, you can check it in the section `External data configuration` in the Details tab.

## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False

**Answer**: False, small tables (<1GB) shouldn't be clustered and if the column has much more rows for a column value (about 80-85%) than the number of rows for the other values. 


## (Bonus: Not worth points) Question 8:
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

**Answer**: 0 bytes, because the total number of rows is in the metadata so there is no need to read the table rows.


# License

Copyright 2023 Eduardo Muñoz

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

