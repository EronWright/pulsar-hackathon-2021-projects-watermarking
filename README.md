_Note: see [EronWright/pulsar](https://github.com/apache/pulsar/compare/branch-2.7...EronWright:watermarking?expand=1) repository (`watermarking` branch) for the corresponding Pulsar code._

## What is Watermarking

Watermarks act as a metric of progress when observing an infinite and unbounded data sourced. A watermark is a notion of input completeness with respect to event times. A watermark(t) declares that event time has reached time t in that stream, meaning that there should be no more elements from the stream with a timestamp t’ <= t (i.e. events with timestamps older or equal to the watermark).

![In-Order Streams](https://ci.apache.org/projects/flink/flink-docs-release-1.13/fig/stream_watermark_in_order.svg)
![Out-of-Order Streams](https://ci.apache.org/projects/flink/flink-docs-release-1.13/fig/stream_watermark_out_of_order.svg)

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

## How it Works
Watermarking is a process of gathering watermark messages from producers, brokering watermarks based on subscriptions, and sending watermarks to consumers.

### Producer API
The producer emits watermarks via a new method on the `Producer` interface:

```java
package org.apache.pulsar.client.api;

public interface Producer<T> extends Closeable {
    /**
     * Create a new watermark builder.
     *
     * @return a watermark builder that can be used to construct the watermark to be sent through this producer.
     */
    WatermarkBuilder newWatermark();
}
```

Watermarks may also be sent transactionally:
```java
public interface Producer<T> extends Closeable {
    /**
     * Create a new watermark builder with transaction.
     */
    WatermarkBuilder newWatermark(Transaction txn);
}
```

The builder allows for an event time to be set on the watermark.
```java
public interface WatermarkBuilder extends Serializable {
    /**
     * Set the event time for a given watermark.
     *
     * @return the watermark builder instance
     */
    WatermarkBuilder eventTime(long timestamp);
    
    CompletableFuture<WatermarkId> sendAsync();
    
    WatermarkId send() throws PulsarClientException;
}
```

The producer is making an assertion that the minimum event time for any subsequent message is at least the specified time (see `setEventTime` field of `TypedMessageBuilder<T>`).

Watermarks are broadcast to all partitions, inline with other messages that are routed normally.  Watermarks are stored in the managed ledger as messages with a `markerType` metadata field with value `W_UPDATE`.

### Demo Producer
The demo app includes a number of producers, each designed to induce a kind of disorder in the event stream.
1. A transactional producer to explore disorder produced by transactions.
2. A producer which produces a partitioned topic.
3. A producer which generates out-of-order messages but with a correct watermark.
4. A producer which generates genuine late messages (i.e. events with a timestamp that is older than the latest watermark).

### Broker
For each subscription, the broker uses a managed cursor to materialize the minimum watermark across all producers. The watermark tracks _acknowledged messages_, so that the watermark doesn't advance prematurely for any one consumer.  This allows the system to work well with all subscription types.  In other words, the watermark cursor tracks the _mark-delete point_ of the subscription's main cursor.

Within the broker, the cursor is encapsulated in a `WatermarkGenerator` that accepts tracking updates from the subscription.
```java
package org.apache.pulsar.broker.service.eventtime;

public interface WatermarkGenerator {
    /**
     * Get the current watermark.
     * @return
     */
    Long getWatermark();

    /**
     * Advance the tracking position of the watermark generator.
     */
    CompletableFuture<Void> seek(Position position);
    
    /**
     * Register a listener for changes to the watermark.
     */
    setListener(WatermarkGeneratorListener listener);
}

```
The watermark generator supports transactions.  If an uncommitted watermark is encountered, the generator holds it in state until the transaction is committed.

The generator vends watermarks to the dispatcher upon changes to the tracking position.  The dispatcher forwards watermarks to all consumers and handles sending the latest watermark to any new consumer.  Again this watermark represents acknowleged messages only, to ensure that the watermark monotonically increases for all consumers.

### Consumer API
The consumer opts into watermarking using a new method on the `ConsumerBuilder`.  There are minor semantic changes to the `read` methods which warrant this.
```java
public interface ConsumerBuilder<T> extends Cloneable {
    /**
     * Enable or disable receiving watermarks.
     * @param watermarkingEnabled true if watermarks should be delivered.
     * @return
     */
    ConsumerBuilder<T> enableWatermarking(boolean watermarkingEnabled);
}
```

The consumer receives watermarks using a new method on the `MessageListener` interface.  

```java
package org.apache.pulsar.client.api;

public interface MessageListener<T> extends Serializable {
    /**
     * This method is called whenever a new watermark is received.
     *
     * <p>Watermarks are guaranteed to be delivered in order (with respect to messages) and from the same thread
     * for a single consumer.
     *
     * <p>This method will only be called once for each watermark, unless either application or broker crashes.
     *
     * <p>Application is responsible of handling any exception that could be thrown while processing the watermark.
     *
     * @param consumer
     *            the consumer that received the message
     * @param watermark
     *            the watermark object
     */
    default void receivedWatermark(Consumer<T> consumer, Watermark watermark) {
        // no-op
    }
}

```
The consumer may also obtain the latest watermark via the `Consumer` interface.  This should be called after `read` or `readAsync` completes.  To accelerate the receipt of watermarks, any outstanding async read is automatically completed with a `null` message.  Open question as to whether an exception would be better.  Suggest apps use `read` with a timeout if the synchronous approach is preferred.

```java
public interface Consumer<T> extends Closeable {
    /**
     * @return The latest watermark, or null if watermarking is not enabled or a watermark has not been received.
     */
    Watermark getLastWatermark();
}
```

The consumer receives watermarks on the same thread as ordinary messages.  The consumer may expect that any subsequent message will have an event timestamp of at least the watermark value.  If the expectation is violated, it is due to a false assertion made by a producer.  The application should treat such messages as _true late messages_.

### Demo Consumer
The demo consumer works by buffering incoming messages into a `PriorityQueue`, sorted by the timestamp of the event.  When a watermark arrives, the consumer flushes from the buffer any event with a timestamp that is older or equal to the watermark.  In this way, a streaming re-ordering of events into event-time order is achieved. 

```java
public class CustomMsgListener implements MessageListener<StationSensorReading> {
    PriorityQueue<StationSensorReading> buffer;
    
    @Override
    public void received(Consumer<StationSensorReading> consumer, Message<StationSensorReading> message) {
        buffer.add(message.getValue());
    }
    
    @Override 
    public void receivedWatermark(Consumer<StationSensorReading> consumer, Watermark watermark) {
        Iterator<StationSensorReading> it = buffer.iterator();
        while(it.hasNext()) {
            StationSensorReading e = it.next();
            if (e.getMeasurementTimestamp().getTime() <= watermark.getEventTime()) {
                System.out.println(e);
                it.remove();
            }
        }
    }
}
```

The demo consumer also has flags to simulate faults.
1. `--fault-noack` - causes the consumer to fail to acknowledge received messages.  In a multi-consumer scenario, this should cause the watermark to stall across all consumers of the subscription.
 
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
