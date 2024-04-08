## Module 6 Homework 

In this homework, we're going to extend Module 5 Homework and learn about streaming with PySpark.

Instead of Kafka, we will use Red Panda, which is a drop-in
replacement for Kafka. 

Ensure you have the following set up (if you had done the previous homework and the module):

- Docker (see [module 1](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform))
- PySpark (see [module 5](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/05-batch/setup))

For this homework we will be using the files from Module 5 homework:

- Green 2019-10 data from [here](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz)



## Start Red Panda

Let's start redpanda in a docker container. 

There's a `docker-compose.yml` file in the homework folder (taken from [here](https://github.com/redpanda-data-blog/2023-python-gsg/blob/main/docker-compose.yml))

Copy this file to your homework directory and run

```bash
docker-compose up
```

(Add `-d` if you want to run in detached mode)


## Question 1: Redpanda version

Now let's find out the version of redpandas. 

For that, check the output of the command `rpk help` _inside the container_. The name of the container is `redpanda-1`.

Find out what you need to execute based on the `help` output.

What's the version, based on the output of the command you executed? (copy the entire version)

**Answer:** v22.3.5 (rev 28b2443)

Once the container is up and runing, you can run the following command to check the version:

```bash
docker exec -ti redpanda-1 rpk version
```

Output:
```bash
@edumunozsala ➜ /workspaces/DE-zoomcamp-course (main) $ docker exec -ti redpanda-1 rpk version
v22.3.5 (rev 28b2443)
@edumunozsala ➜ /workspaces/DE-zoomcamp-course (main) $
```

## Question 2. Creating a topic

Before we can send data to the redpanda server, we
need to create a topic. We do it also with the `rpk`
command we used previously for figuring out the version of 
redpandas.

Read the output of `help` and based on it, create a topic with name `test-topic` 

What's the output of the command for creating a topic? Include the entire output in your answer.

**Answer**:

We checkm the documentation of rpk: https://docs.redpanda.com/current/reference/rpk/rpk-topic/rpk-topic-create/

We execute the following command, with default settings:
```bash
rpk topic create test-topic
```


Full output:
```bash
redpanda@8e580eb6ea11:/$ rpk topic create test-topic
TOPIC       STATUS
test-topic  OK
redpanda@8e580eb6ea11:/$
```

## Question 3. Connecting to the Kafka server

We need to make sure we can connect to the server, so
later we can send some data to its topics

First, let's install the kafka connector (up to you if you
want to have a separate virtual environment for that)

```bash
pip install kafka-python
```

You can start a jupyter notebook in your solution folder or
create a script

Let's try to connect to our server:

```python
import json
import time 

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()
```

Provided that you can connect to the server, what's the output
of the last command?

**Answer**: True

We run the code in a jupyter notebook running locally.




## Question 4. Sending data to the stream

Now we're ready to send some test data:

```python
t0 = time.time()

topic_name = 'test-topic'

for i in range(10):
    message = {'number': i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
```

How much time did it take? Where did it spend most of the time?

* Sending the messages
* Flushing
* Both took approximately the same amount of time

(Don't remove `time.sleep` when answering this question)

**Answer**: Sending the messages (if we include the sleep time)

We run the code in a jupyter notebook and this was the cell output:
```text
Sent: {'number': 0}
Sent: {'number': 1}
Sent: {'number': 2}
Sent: {'number': 3}
Sent: {'number': 4}
Sent: {'number': 5}
Sent: {'number': 6}
Sent: {'number': 7}
Sent: {'number': 8}
Sent: {'number': 9}
took 0.50 seconds
```

If we remove the flush command it takes about the same time. We can observe that the execution time is the sleeping time * 10, so the time is spent in sleeping. If we include the sleep time in the "Sending the messages" time then it takes mosty of the time.



## Reading data with `rpk`

You can see the messages that you send to the topic
with `rpk`:

```bash
rpk topic consume test-topic
```

Run the command above and send the messages one more time to 
see them

We a set of messages:
```txt
{
  "topic": "test-topic",
  "value": "{\"number\": 7}",
  "timestamp": 1712567850945,
  "partition": 0,
  "offset": 47
}
{
  "topic": "test-topic",
  "value": "{\"number\": 8}",
  "timestamp": 1712567850945,
  "partition": 0,
  "offset": 48
}
{
  "topic": "test-topic",
  "value": "{\"number\": 9}",
  "timestamp": 1712567850945,
  "partition": 0,
  "offset": 49
}
```


## Sending the taxi data

Now let's send our actual data:

* Read the green csv.gz file
* We will only need these columns:
  * `'lpep_pickup_datetime',`
  * `'lpep_dropoff_datetime',`
  * `'PULocationID',`
  * `'DOLocationID',`
  * `'passenger_count',`
  * `'trip_distance',`
  * `'tip_amount'`

Iterate over the records in the dataframe

```python
for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    print(row_dict)
    break

    # TODO implement sending the data here
```

Note: this way of iterating over the records is more efficient compared
to `iterrows`


## Question 5: Sending the Trip Data

* Create a topic `green-trips` and send the data there
* How much time in seconds did it take? (You can round it to a whole number)
* Make sure you don't include sleeps in your code

**Answer**: 68.44 seconds

First, we create a new topic called `green-rides-201910` with default config (just 1 partition):

```bash
rpk topic create green-rides-201910
```

Thewn, we can run the following code to send the dataframe to the kafka topic created:
```python
t0 = time.time()

topic_name = 'green-rides-201910'

for row in df.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    producer.send(topic_name, value=row_dict)
    print(f"Sent: {row_dict}")

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
```

The output:
```text
Sent: {'lpep_pickup_datetime': '2019-10-31 23:30:00', 'lpep_dropoff_datetime': '2019-11-01 00:00:00', 'PULocationID': 65, 'DOLocationID': 102, 'passenger_count': nan, 'trip_distance': 7.04, 'tip_amount': 0.0}
Sent: {'lpep_pickup_datetime': '2019-10-31 23:03:00', 'lpep_dropoff_datetime': '2019-10-31 23:24:00', 'PULocationID': 129, 'DOLocationID': 136, 'passenger_count': nan, 'trip_distance': 0.0, 'tip_amount': 0.0}
Sent: {'lpep_pickup_datetime': '2019-10-31 23:02:00', 'lpep_dropoff_datetime': '2019-10-31 23:23:00', 'PULocationID': 61, 'DOLocationID': 222, 'passenger_count': nan, 'trip_distance': 3.9, 'tip_amount': 0.0}
Sent: {'lpep_pickup_datetime': '2019-10-31 23:42:00', 'lpep_dropoff_datetime': '2019-10-31 23:56:00', 'PULocationID': 76, 'DOLocationID': 39, 'passenger_count': nan, 'trip_distance': 3.08, 'tip_amount': 0.0}
Sent: {'lpep_pickup_datetime': '2019-10-31 23:23:00', 'lpep_dropoff_datetime': '2019-10-31 23:56:00', 'PULocationID': 56, 'DOLocationID': 215, 'passenger_count': nan, 'trip_distance': 6.84, 'tip_amount': 0.0}
took 68.44 seconds
```

## Creating the PySpark consumer

Now let's read the data with PySpark. 

Spark needs a library (jar) to be able to connect to Kafka, 
so we need to tell PySpark that it needs to use it:

```python
import pyspark
from pyspark.sql import SparkSession

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()
```

Now we can connect to the stream:

```python
green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()
```

In order to test that we can consume from the stream, 
let's see what will be the first record there. 

In Spark streaming, the stream is represented as a sequence of 
small batches, each batch being a small RDD (or a small dataframe).

So we can execute a function over each mini-batch.
Let's run `take(1)` there to see what do we have in the stream:

```python
def peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)

    if first_row:
        print(first_row[0])

query = green_stream.writeStream.foreachBatch(peek).start()
```

You should see a record like this:

```
Row(key=None, value=bytearray(b'{"lpep_pickup_datetime": "2019-10-01 00:26:02", "lpep_dropoff_datetime": "2019-10-01 00:39:58", "PULocationID": 112, "DOLocationID": 196, "passenger_count": 1.0, "trip_distance": 5.88, "tip_amount": 0.0}'), topic='green-trips', partition=0, offset=0, timestamp=datetime.datetime(2024, 3, 12, 22, 42, 9, 411000), timestampType=0)
```

Now let's stop the query, so it doesn't keep consuming messages
from the stream

```python
query.stop()
```

## Question 6. Parsing the data

The data is JSON, but currently it's in binary format. We need
to parse it and turn it into a streaming dataframe with proper
columns.

Similarly to PySpark, we define the schema

```python
from pyspark.sql import types

schema = types.StructType() \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())
```

And apply this schema:

```python
from pyspark.sql import functions as F

green_stream = green_stream \
  .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
  .select("data.*")
```

How does the record look after parsing? Copy the output. 

**Answer**:
```text
Row(lpep_pickup_datetime='2019-10-01 00:26:02', lpep_dropoff_datetime='2019-10-01 00:39:58',PULocationID=112, DOLocationID=196, passenger_count=1.0, trip_distance=5.88, tip_amount=0.0)
```

### Question 7: Most popular destination

Now let's finally do some streaming analytics. We will
see what's the most popular destination currently 
based on our stream of data (which ideally we should 
have sent with delays like we did in workshop 2)


This is how you can do it:

* Add a column "timestamp" using the `current_timestamp` function
* Group by:
  * 5 minutes window based on the timestamp column (`F.window(col("timestamp"), "5 minutes")`)
  * `"DOLocationID"`
* Order by count

You can print the output to the console using this 
code

```python
query = popular_destinations \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
```

Write the most popular destination, your answer should be *either* the zone ID or the zone name of this destination. (You will need to re-send the data for this to work)

**Answer**: 74

The output cell:
```text
24/03/21 21:14:22 WARN ResolveWriteToStream: Temporary checkpoint location created which is deleted normally when the query didn't fail: /tmp/temporary-4b21ef59-79a3-4b51-8c9a-39cc30542168. If it's required to delete it under any circumstances, please set spark.sql.streaming.forceDeleteTempCheckpointLocation to true. Important to know deleting temp checkpoint folder is best effort.
24/03/21 21:14:22 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
                                                                                
-------------------------------------------
Batch: 0
-------------------------------------------
+------------------------------------------+------------+-----+
|window                                    |DOLocationID|count|
+------------------------------------------+------------+-----+
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|74          |53223|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|42          |47826|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|41          |42183|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|75          |38520|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|129         |35790|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|7           |34599|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|166         |32535|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|236         |23739|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|223         |22626|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|238         |21954|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|82          |21876|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|181         |21846|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|95          |21732|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|244         |20199|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|61          |19818|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|116         |19017|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|138         |18432|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|97          |18150|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|49          |15663|
|{2024-03-21 21:10:00, 2024-03-21 21:15:00}|151         |15459|
+------------------------------------------+------------+-----+
only showing top 20 rows

``` 

