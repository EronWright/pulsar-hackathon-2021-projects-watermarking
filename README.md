# Pulsar Hackathon 2021 Watermarking Project

Scope
-----
This repository contains sample code in order to experiment with the support of watermarks in Apache Pulsar.
Watermarks may be understood as an assertion made by the producers about the progression of event time and 
the end goal is to provide correctness of results for the end consumers - i.e in an infinite stream of events where they may arrive out of order.

Dataset
-------
For illustration purposes we use a sensor dataset, that provides many out-of-order events.
More information can be found here:\
https://data.world/cityofchicago/beach-weather-stations-automated-sensors

Producers
---------
One the producer side, we have 4 different use cases:
1. **SyncProducer:** A simple producer that ingests the data in pulsar a creates a watermark upon completion
2. **MultiProducerSync:** We create a producer per **station (3 producers)**, where each generates it's own watermark
3. **MultipleProducersPartitionedSync:** We ingest a data into a partitioned topic, where each topic has data for a specific station and each has it's own watermarks
4. **TransactionalProducer:** We use the transactional functionality of pulsar, that periodically commits transactions and upon every commit it creates a watermark

Consumers
---------
The end goal for a consumer, for illustration purposes is to consume events from a topic.
While consuming the events, the consumer holds those events in a buffer waiting to receive a watermark.
When the consumer receives a watermark, then it materializes the events stored in the buffer - sorts those events
and emits them for downstream consumption.

1. **Java Client Consumer**

2. **Apache Flink**
