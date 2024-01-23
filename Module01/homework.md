## Module 1 Homework

## Docker & SQL

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits* 

- `--delete`
- `--rc`
- `--rmc`
- `--rm`

**Answer**
The tag --rm


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- 0.42.0
- 1.0.0
- 23.0.1
- 58.1.0

**Answer**
Command: docker run -ti python:3.9 bash
wheel package version: 0.42.0


# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)


## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 15767
- 15612
- 15859
- 89009

**Answer**
```sql
SELECT COUNT(*)
FROM 
	green_taxi_trips
WHERE
	CAST(lpep_pickup_datetime AS DATE)='2019-09-18' and
	CAST(lpep_dropoff_datetime AS DATE)='2019-09-18'
```
Result: 15612

## Question 4. Largest trip for each day

Which was the pick up day with the largest trip distance
Use the pick up time for your calculations.

- 2019-09-18
- 2019-09-16
- 2019-09-26
- 2019-09-21

**Answer**
```sql
SELECT 
	CAST(lpep_pickup_datetime AS DATE) AS "day",
	MAX(trip_distance)
FROM 
	green_taxi_trips
GROUP BY CAST(lpep_pickup_datetime AS DATE)
ORDER BY MAX(trip_distance) DESC
```

Result: 2019-09-26 Max trip distance: 341.64

## Question 5. The number of passengers

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
 
- "Brooklyn" "Manhattan" "Queens"
- "Bronx" "Brooklyn" "Manhattan"
- "Bronx" "Manhattan" "Queens" 
- "Brooklyn" "Queens" "Staten Island"

**ANSWER**
```sql
SELECT 
	zpu."Borough",
	SUM(t.total_amount)
FROM 
	green_taxi_trips t JOIN zones zpu 
		ON t."PULocationID"= zpu."LocationID"
WHERE 
	CAST(lpep_pickup_datetime AS DATE)='2019-09-18' AND
	zpu."Borough"!='Unknown'
GROUP BY
	zpu."Borough"
HAVING
	SUM(t.total_amount)>50000
```

Result:
    "Brooklyn"	96333.24000000063
    "Manhattan"	92271.30000000144
    "Queens"	78671.70999999958


## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- JFK Airport
- Long Island City/Queens Plaza

**ANSWER**
```sql
SELECT 
	zdo."Zone",
	MAX(t.tip_amount)
FROM 
	green_taxi_trips t JOIN zones zpu 
		ON t."PULocationID"= zpu."LocationID"
	JOIN zones zdo ON t."DOLocationID"= zdo."LocationID"
WHERE 
	TO_CHAR(lpep_pickup_datetime, 'YYYY')='2019' AND
	TO_CHAR(lpep_pickup_datetime, 'MM')='09' AND
	zpu."Zone"='Astoria'
GROUP BY
	zdo."Zone"
ORDER BY 
	MAX(t.tip_amount) DESC
```
Result:
**"JFK Airport"**	62.31
"Woodside"	30
"Kips Bay"	28
"NV"	25

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.

**Answer**
```text
@edumunozsala ➜ /workspaces/DE-zoomcamp-course/terraform/variables (main) $ terraform apply

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.demo_dataset will be created
  + resource "google_bigquery_dataset" "demo_dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "emsbigqdsdezc"
      + default_collation          = (known after apply)
      + delete_contents_on_destroy = false
      + effective_labels           = (known after apply)
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + is_case_insensitive        = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "US"
      + max_time_travel_hours      = (known after apply)
      + project                    = "banded-pad-411315"
      + self_link                  = (known after apply)
      + storage_billing_model      = (known after apply)
      + terraform_labels           = (known after apply)
    }

  # google_storage_bucket.demo-bucket will be created
  + resource "google_storage_bucket" "demo-bucket" {
      + effective_labels            = (known after apply)
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "US"
      + name                        = "emsdezoomcamp"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + terraform_labels            = (known after apply)
      + uniform_bucket_level_access = (known after apply)
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "AbortIncompleteMultipartUpload"
            }
          + condition {
              + age                   = 1
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/banded-pad-411315/datasets/emsbigqdsdezc]
google_storage_bucket.demo-bucket: Creation complete after 1s [id=emsdezoomcamp]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```


## Submitting the solutions

* Form for submitting: 
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: