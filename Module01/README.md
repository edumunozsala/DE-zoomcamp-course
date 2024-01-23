# Data Engineering Zoomcamp Course 2024
# Module 1: Containerization and Infrastructure as Code

Repo for tasks and homeworks included in the Module 1: Data Engineering Zoomcamp course Cohort 2024.

#### Repo still in progress

## Content

- Introduction to GCP
- Docker and docker-compose
- Running Postgres locally with Docker
- Running Postgres and pgadmin in containers using docker compose
- Build a ingestion data pipeline running in a container
- Introduction to Terraform
- Setting up infrastructure on GCP with Terraform

## Run a Postgres database and the pgadmin UI in containers

To run a Postgress database and the pgadmin interface execute the docker compose yml file:
```
docker compose up
```

This command launch a docker container with Postgres, listening in port 5432, and then another docker container with the pgadmin web interface in the port 80.
You can browse in [url](http://localhost:80).

Connect pgadmin to Postgres database `pg-database` using the credentials in the `docker-compose.yml` file.

## Run a data ingestion script in its own container

Once the Postgres database is up, you can run a container to ingest data to the database from an URL. In this case we use a csv file containing data from the NYC Taxi and Limousine Commision. You can find example files in [NYC NLC Data](https://github.com/DataTalksClub/nyc-tlc-data).

To ingest the data run the docker command:
```
# Build the image
docker build -t taxi_ingest:v001 .

# Run the loading script for the HOMEWORK
# Set the URL to the csv file, data from Sep 2019
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"

# run the container
docker run -it \
  --network=module01_pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL}

```

## Terraform and GCP
In the Terraform directory you can find a basic example on how to start services in Google Cloud Platform using Terraform.

Just set a `variables.tf` file with the variables and the `main.tf` to create a Cloud Storage Bucket and a BigQuery dataset, and then run the terraform commands:

1. `terraform init`:
    Initializes & configures the backend, installs plugins/providers, & checks out an existing configuration from a version control

2. `terraform plan`:
    Matches/previews local changes against a remote state, and proposes an Execution Plan.

3. `terraform apply`:
    Asks for approval to the proposed plan, and applies changes to cloud

4. `terraform destroy`:
    Removes your stack from the Cloud


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

