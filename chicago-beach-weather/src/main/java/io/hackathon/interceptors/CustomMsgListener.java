package io.hackathon.interceptors;

import io.hackathon.models.StationSensorReading;
import org.apache.pulsar.client.api.*;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomMsgListener implements MessageListener<StationSensorReading> {
    private final AtomicInteger messageCounter = new AtomicInteger();
    Comparator<StationSensorReading> datetimeSorter;
    PriorityQueue<StationSensorReading> buffer;
    private boolean ackMessage;

    public CustomMsgListener(boolean ackMessage){
        datetimeSorter = Comparator.comparing(StationSensorReading::getMeasurementTimestamp);
        buffer = new PriorityQueue<StationSensorReading>(datetimeSorter);
        this.ackMessage = ackMessage;
    }

    @Override
    public void received(Consumer<StationSensorReading> consumer, Message<StationSensorReading> message) {
        System.out.printf("received: %s (%d)\n", message.getMessageId(), message.getEventTime());

        try {
            buffer.add(message.getValue());
            if(this.ackMessage)
                consumer.acknowledge(message);

        } catch (PulsarClientException e) {
            consumer.negativeAcknowledge(message);
        }
    }

    @Override
    public void reachedEndOfTopic(Consumer<StationSensorReading> consumer) {
        System.out.println("Consumer " + consumer.getConsumerName() + " reached the end of the topic.");
    }

    @Override
    public void receivedWatermark(Consumer<StationSensorReading> consumer, Watermark watermark) {
        System.out.println("Consumer " + consumer.getConsumerName() + " received watermark: " + watermark.getEventTime());

        Iterator<StationSensorReading> it = buffer.iterator();
        while(it.hasNext()) {
            StationSensorReading e = it.next();
            if (e.getMeasurementTimestamp().getTime() <= watermark.getEventTime()) {
                // the event time is before the watermark; flush the event.
                System.out.println(e);
                it.remove();
            }
        }
    }
}
