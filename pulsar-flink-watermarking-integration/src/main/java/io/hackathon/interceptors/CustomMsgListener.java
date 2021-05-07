package io.hackathon.interceptors;

import io.hackathon.models.StationSensorReading;
import org.apache.pulsar.client.api.*;

import java.util.concurrent.atomic.AtomicInteger;

public class CustomMsgListener implements MessageListener<StationSensorReading> {
    private final AtomicInteger messageCounter = new AtomicInteger();

    @Override
    public void received(Consumer<StationSensorReading> consumer, Message<StationSensorReading> message) {
        System.out.printf("Message received: %s%n", new String(message.getData()));
        try {
            consumer.acknowledge(message);
        } catch (PulsarClientException e) {
            consumer.negativeAcknowledge(message);
        }
        System.out.println("Total Messages Received: " + messageCounter.getAndIncrement());
    }

    @Override
    public void reachedEndOfTopic(Consumer<StationSensorReading> consumer) {
        System.out.println("Consumer " + consumer.getConsumerName() + "reached the end of the topic.");
    }

    @Override
    public void receivedWatermark(Consumer<StationSensorReading> consumer, Watermark watermark) {
        System.out.println("Consumer " + consumer.getConsumerName() + "received watermark " + watermark.toString());
    }
}
