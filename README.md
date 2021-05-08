_Note: see [EronWright/pulsar](https://github.com/apache/pulsar/compare/branch-2.7...EronWright:watermarking?expand=1) repository (`watermarking` branch) for the corresponding Pulsar code._

## What is Watermarking

Watermarks act as a metric of progress when observing an infinite and unbounded data sourced. A watermark is a notion of input completeness with respect to event times. A watermark(t) declares that event time has reached time t in that stream, meaning that there should be no more elements from the stream with a timestamp t’ <= t (i.e. events with timestamps older or equal to the watermark).

Watermarks are crucial for out-of-order streams, where the events are not strictly ordered by their timestamps.

For more details about watermark concepts, see O’Reilly’s [Beyond Batch 101](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/) and [Beyond Batch 102](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/) article series. 

## Why We Develop Watermarking for Pulsar

Pulsar has a limited concept of event time: the `eventTime` field of a message. This field (or a timestamp extracted from the message body) is sometimes used by the consumer to generate watermarks, for example, using the bounded out-of-orderness heuristic. However, this approach is highly sensitive to Pulsar’s system internals, especially in historical processing and when working with partitioned topics. So there are some problems of generating watermarks when consuming a Pulsar topic by Flink application, Pulsar Functions, and other and other modern stream processing engines (SPE):

- Pulsar does not guarantee ordering across keys nor with respect to event time. It means that a consumer may observe a highly disordered stream of events with respect to the event time domain.
- Pulsar has no concept of producer groups and no way to enumerate which producers are active on a topic when there are multiple producers.
- Pulsar client encapsulation makes it difficult for an application to generate correct watermarks, and the client makes no effort to deliver messages in event-time order or to emphasize fairness.
- Pulsar transactions increase disorder in the event time domain from a subscriber perspective due to batching effects.

Our goal is to support watermark in Apache Pulsar, encapsulate system internals and shift the burden of watermark correctness to the problem domain. The producers creates watermarks to mark the progression of event time. For an infinite stream of events where messages and watermarks arrive out of order, Apache Pulsar is able to provide correct results for the end consumers. 

For more details, see [our proposal](https://docs.google.com/document/d/1fWOM1zYpBrbVRI02r5w91Wtb3Ou65905abv-1242Vb8/edit#heading=h.a2h18vcro4v8).

## How to Experiment

You can experiment the watermarking progress with the sample code contained in the [this watermarking repository](https://github.com/EronWright/pulsar-hackathon-2021-projects-watermarking). In our experiment, we adopt a sensor dataset that provides many out-of-order events. For details on the data, refer to [our dataset](https://data.world/cityofchicago/beach-weather-stations-automated-sensors).

### Producers

From the producer side, we set up four use cases:

1. **SyncProducer**: A producer that ingests data in Pulsar and creates a watermark when completing data ingestion.
2. **MultiProducerSync**: Each **station** has one producer and each producer generates its own watermark. We have three **stations** and three producers.
3. **MultipleProducersPartitionedSync**: Data are ingested by a partitioned topic, and each topic has data for a specific station and each has its own watermark.
4. **TransactionalProducer**: By using Pulsar transactions, watermarks are created when periodically committing transactions. 

### Consumers

The consumers consume events from a topic in the following way:   

1. The consumer stores the events in a buffer until it receives a watermark.
2. When the consumer receives a watermark, the consumer materializes the events stored in the buffer.
3. The consumer then sorts and emits the events for downstream consumption.

In our experiment, we support the following consumers:

- Java Client Consumer
- Apache Flink

## Conclusion

It is worthy to introduce watermarks in Apache Pulsar, the main benefits include: 

- Shift burden of watermark correctness to the problem domain.
- Ensure time-ordered stream of events for the end consumers for an infinite stream of events, even if events arrive out-of-order in Pulsar.

If you're interested in the watermarking project, feel free to share your ideas in [issues](https://github.com/EronWright/pulsar-hackathon-2021-projects-watermarking/issues) or make contributions by [submitting PRs](https://github.com/EronWright/pulsar-hackathon-2021-projects-watermarking/pulls).

## Reference

The following are some references we've used in this project:

- [Watermark proposal](https://docs.google.com/document/d/1fWOM1zYpBrbVRI02r5w91Wtb3Ou65905abv-1242Vb8/edit#heading=h.a2h18vcro4v8)
- [Data set](https://data.world/cityofchicago/beach-weather-stations-automated-sensors)
- [Beyond Batch 101](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/)
- [Beyond Batch 102](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/)
