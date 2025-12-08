# Homework

In this homework, I learned how to work with streaming data with PyFlink and Redpanda. Here I want to show you how I completed the whole task.

## Setup

First, I needed to get all the required containers running:

```bash
docker-compose up -d
```

This started several containers:

* Redpanda (Kafka replacement)
* Flink Job Manager
* Flink Task Manager
* PostgreSQL
* pgAdmin

I visited http://localhost:8081 to see the Flink Job Manager, and accessed pgAdmin at http://localhost:8080 with:

* Email: admin@admin.com
* Password:admin

Then I connected to PostgreSQL with these settings:

* Host: postgres
* Port: 5432
* Username: postgres
* Password: postgres

## Question 1: Redpanda version

Now let's find out the version of redpandas.

For that, check the output of the command `rpk help` _inside the container_. The name of the container is `redpanda-1`.

Find out what you need to execute based on the `help` output.

What's the version, based on the output of the command you executed? (copy the entire version)

**Solution:**

I ran the following command to check the version of Redpanda:

```bash
docker exec -it redpanda-1 bash
rpk version
```

**Answer**: v24.2.18

## Question 2. Creating a topic

Before we can send data to the redpanda server, we
need to create a topic. We do it also with the `rpk`
command we used previously for figuring out the version of
redpandas.

Read the output of `help` and based on it, create a topic with name `green-trips`

What's the output of the command for creating a topic? Include the entire output in your answer.

**Solution:**

Before sending data, I needed to create a Kafka topic. So I ran:

```bash
docker exec -it redpanda-1 rpk topic create green-trips
```

**Answer**: TOPIC        STATUS
green-trips  OK

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

**Solution:**

I installed the kafka Python library:

```bash
pip install kafka-python
```

Then I created connection.py to test if I could connect to the Redpanda serve. I ran:

```bash
python connection.py
```

**Answer**: True

## Question 4: Sending the Trip Data

Now we need to send the data to the `green-trips` topic

Read the data, and keep only these columns:

* `'lpep_pickup_datetime',`
* `'lpep_dropoff_datetime',`
* `'PULocationID',`
* `'DOLocationID',`
* `'passenger_count',`
* `'trip_distance',`
* `'tip_amount'`

Now send all the data using this code:

```python
producer.send(topic_name, value=message)
```

For each row (`message`) in the dataset. In this case, `message`
is a dictionary.

After sending all the messages, flush the data:

```python
producer.flush()
```

Use `from time import time` to see the total time

```python
from time import time

t0 = time()

# ... your code

t1 = time()
took = t1 - t0
```

How much time did it take to send the entire dataset and flush?
**Solution:**

```bash
python send_trips.py
```

**Answer**: 32.77 seconds to send all the data

## Question 5: Build a Sessionization Window (2 points)

Now we have the data in the Kafka stream. It's time to process it.

* Copy `aggregation_job.py` and rename it to `session_job.py`
* Have it read from `green-trips` fixing the schema
* Use a [session window](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/windows/) with a gap of 5 minutes
* Use `lpep_dropoff_datetime` time as your watermark with a 5 second tolerance
* Which pickup and drop off locations have the longest unbroken streak of taxi trips?

**Solution**

1. Create session_job.py to process the streaming data with a 5-minute session window;
2. Create a table taxi_sessions in pgAdmin:

   ```sql
   CREATE TABLE taxi_sessions (
       pu_location_id INT,
       do_location_id INT,
       trip_count BIGINT,
       window_start TIMESTAMP(3),
       window_end TIMESTAMP(3),
       PRIMARY KEY (pu_location_id, do_location_id, window_start)
   );
   ```
3. Copy it to the Flink container:

   ```bash
   docker cp src/job/session_job.py flink-jobmanager:/opt/flink/session_job.py
   ```
4. Run the Flink job:

   ```bash
   docker exec -it flink-jobmanager /opt/flink/bin/flink run -py /opt/flink/session_job.py
   ```
5. Run the query:

   ```sql
   WITH session_durations AS (
   SELECT
   pu_location_id,
   do_location_id,
   window_start,
   window_end,
   trip_count,
   EXTRACT(EPOCH FROM (window_end - window_start))/60 as duration_minutes
   FROM taxi_sessions
   )
   SELECT
   pu_location_id,
   do_location_id,
   window_start,
   window_end,
   trip_count,
   duration_minutes
   FROM session_durations
   ORDER BY duration_minutes DESC
   LIMIT 10;
   ```

**Answer**:

I found that pickup location 74 and dropoff location 75 had the longest unbroken streak of taxi trips. This streak lasted for 70.53 minutes (from 2019-10-21 08:48:10 to 2019-10-21 09:58:42) and included 31 trips.