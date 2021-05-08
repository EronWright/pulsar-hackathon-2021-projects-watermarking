## What is Watermarker

## Why we develop watermark for Pulsar

## Scope

Our goal is to support watermarks in Apache Pulsar. The producers creates watermarks to mark the progression of event time. For an infinite stream of events where messages and watermarks arrive out of order, Apache Pulsar is able to provide correct results for the end consumers.

You can experiment the watermarking progress with the sample code contained in the repository.

## Dataset

In our experiment, we adopt a sensor dataset that provides many out-of-order events. For details, refer to [our dataset](https://data.world/cityofchicago/beach-weather-stations-automated-sensors).

## Producers

From the producer side, we set up four use cases:

1. **SyncProducer**: A producer that ingests data in Pulsar and creates a watermark when completing data ingestion.
2. **MultiProducerSync**: Each **station** has one producer and each producer generates its own watermark. We have three **stations** and three producers.
3. **MultipleProducersPartitionedSync**: Data are ingested by a partitioned topic, and each topic has data for a specific station and each has its own watermark.
4. **TransactionalProducer**: We use the transactional functionality of pulsar, that periodically commits transactions and upon every commit it creates a watermark. By using Pulsar transactions, watermarks are created when periodically committing transactions. 

## Consumers

The consumers consume events from a topic in the following way.   

1. The consumer stores the events in a buffer until it receives a watermark.
2. When the consumer receives a watermark, the consumer materializes the events stored in the buffer.
3. The consumer then sorts and emits the events for downstream consumption.

In our experiment, we support the following consumers:

- Java Client Consumer
- Apache Flink

## Conclusion

