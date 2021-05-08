package io.hackathon.producer;

import io.hackathon.config.AppConfig;
import io.hackathon.models.StationSensorReading;
import io.hackathon.utils.AppUtils;
import io.hackathon.utils.ClientUtils;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class MultipleProducersSync {
    public static void main(String[] args) throws IOException {
        List<StationSensorReading> stationSensorReadings = AppUtils.loadStationSensorReadingsData();
        HashMap<String, List<StationSensorReading>> stationData = AppUtils.groupReadingsByStation(stationSensorReadings);
        PulsarClient pulsarClient = ClientUtils.initPulsarClient();

        // create a producer per station
        stationData.forEach((key, values) -> {
            CompletableFuture.supplyAsync(() -> {
                try {
                    Producer<StationSensorReading> producer = ClientUtils.initSimpleProducer(pulsarClient, AppConfig.topicNameSingle, key + "-producer");
                    int totalMessages = 0;
                    long t1 = System.currentTimeMillis();
                    Timestamp currentWatermark = values.get(0).getMeasurementTimestamp();

                    for (StationSensorReading ssr : values) {
                        if (ssr.getMeasurementTimestamp().after(currentWatermark)) {
                            currentWatermark = ssr.getMeasurementTimestamp();
                        }

                        MessageId messageId = producer
                                .newMessage(JSONSchema.of(StationSensorReading.class))
                                .key(ssr.getStationName())
                                .value(ssr)
                                .send();

                        System.out.printf("Producer %s - sent message with id: '%s' and payload: %s%n", producer.getProducerName(), messageId.toString(), ssr);
                        totalMessages += 1;
                    }
                    long t2 = System.currentTimeMillis();
                    long totalTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(t2 - t1);
                    producer.flush();
                    System.out.printf("Producer %s - Total messages produced: %s in %s seconds.%n", producer.getProducerName(), totalMessages, totalTimeSeconds);

                    System.out.printf("Producer %s - Sending watermark: %s%n", producer.getProducerName(), currentWatermark.toString());
                    WatermarkId watermarkId = producer.newWatermark()
                            .eventTime(currentWatermark.getTime())
                            .send();

                    System.out.printf("Producer %s sent watermark %s successfully.%n", producer.getProducerName(), watermarkId.toString());
                    return producer;
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                    return null;
                }
            }).whenCompleteAsync((stationSensorReadingProducer, throwable) -> {
                if (throwable != null) {
                    System.out.println("Producer " + stationSensorReadingProducer.getProducerName() + " completed successfully, now closing");
                    try {
                        stationSensorReadingProducer.close();
                    } catch (PulsarClientException e) {
                        e.printStackTrace();
                    }
                } else {
                    throwable.printStackTrace();
                }
            });
        });


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing client ...");
            try {
                pulsarClient.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }));

    }
}
