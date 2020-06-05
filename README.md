# SparkCrimeStatistics

## Purpose

An application to monitor statistical analysis of San Francisco crime incidents, using Kafka server for data producing and Spark Structured Streaming for data ingesting. [https://www.udacity.com/course/data-streaming-nanodegree--nd029]

## Usage

1. Start Zookeeper `/usr/bin/zookeeper-server-start config/zookeeper.properties`
2. Start Kafka `/usr/bin/kafka-server-start config/server.properties`
3. Produce messages `python kafka_server.py`
4. Run Spark `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] --conf spark.ui.port=3000 data_stream.py`

## Walkthrough

Developing steps:

1. [Create simple Kafka Producer client](https://github.com/jadugnap/spark-crime-statistics/commit/08296c0963fecf733e73e969575f0038e729d81f "link to this commit")
2. [Finish data_stream core logic](https://github.com/jadugnap/spark-crime-statistics/commit/c5869242aca73bfeb24eea922588a9e7ccd38970 "link to this commit")

Testing steps:

1. To produce -> `python kafka_server.py`
2. To consume -> `sh consume_kafka.sh`
3. To check streaming logic -> `data_stream.ipynb`

### Kafka Consumer
![kafka-consumer](https://github.com/jadugnap/spark-crime-statistics/blob/master/images/KafkaConsumer.png)

### Spark Progress Report
![crime-statistics](https://github.com/jadugnap/spark-crime-statistics/blob/master/images/SparkCount.png)
![progress-1of2](https://github.com/jadugnap/spark-crime-statistics/blob/master/images/Progress-1of2.png)
![progress-2of2](https://github.com/jadugnap/spark-crime-statistics/blob/master/images/Progress-2of2.png)

### Spark UI
![spark-ui](https://github.com/jadugnap/spark-crime-statistics/blob/master/images/SparkUI.png)

## Discussion

#### How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
- `processedRowsPerSecond` indicates the rate at which Spark is processing data.
- This is the most significant variable to optimize.

#### What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

1. Default shuffle partitions
Input:
- `spark.sql.shuffle.partitions` = 200
Output:
- `processedRowsPerSecond` : 10.057616496512685

2. Half shuffle partitions
Input:
- `spark.sql.shuffle.partitions` = 100
Output:
- `processedRowsPerSecond` : 12.884428617675622

3. Quarter shuffle partitions
Input:
- `spark.sql.shuffle.partitions` = 50
Output:
- `processedRowsPerSecond` : 15.890760999760442
