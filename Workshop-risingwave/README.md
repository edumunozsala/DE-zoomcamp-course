# Data Engineering Zoomcamp Course 2024
# Workshop: Stream processing RisingWave

## Stream processing in SQL with RisingWave

In this hands-on workshop, we’ll learn how to process real-time streaming data using SQL in RisingWave. The system we’ll use is RisingWave, an open-source SQL database for processing and managing streaming data. You may not feel unfamiliar with RisingWave’s user experience, as it’s fully wire compatible with PostgreSQL.

We’ll cover the following topics in this Workshop:

- Why Stream Processing?
- Stateless computation (Filters, Projections)
- Stateful Computation (Aggregations, Joins)
- Data Ingestion and Delivery

RisingWave in 10 Minutes:
https://tutorials.risingwave.com/docs/intro

## Workshop

You can get into the workshop in this [GitHub repo](https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04/tree/main)

## Prerequisites

1. Docker and Docker Compose
2. Python 3.7 or later
3. `pip` and `virtualenv` for Python
4. `psql` (I use PostgreSQL-14.9)
5. Clone this repository:
   ```sh
   git clone git@github.com:risingwavelabs/risingwave-data-talks-workshop-2024-03-04.git
   cd risingwave-data-talks-workshop-2024-03-04
   ```
   Or, if you prefer HTTPS:
   ```sh
   git clone https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04.git
   cd risingwave-data-talks-workshop-2024-03-04
   ```

## Note on the dataset

The NYC Taxi dataset is a public dataset that contains information about taxi trips in New York City.
The dataset is available in Parquet format and can be downloaded from the [NYC Taxi & Limousine Commission website](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

We will be using the following files from the dataset:
- `yellow_tripdata_2022-01.parquet`
- `taxi_zone.csv`

For your convenience, these have already been downloaded and are available in the `data` directory.

The file `seed_kafka.py` contains the logic to process the data and populate RisingWave.

In this workshop, we will replace the `timestamp` fields in the `trip_data` with `timestamp`s close to the current time.
That's because `yellow_tripdata_2022-01.parquet` contains historical data from 2022,
and we want to simulate processing real-time data.

## Getting Started

Before getting your hands dirty with the project, we will:
1. Run some diagnostics.
2. Start the RisingWave cluster.
3. Setup our python environment.

```bash
# Check version of psql
psql --version
source commands.sh

# Start the RW cluster
start-cluster

# Setup python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`commands.sh` contains several commands to operate the cluster. You may reference it to see what commands are available.

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```

## Homework

Now, we can start the [homework](homework.md)

