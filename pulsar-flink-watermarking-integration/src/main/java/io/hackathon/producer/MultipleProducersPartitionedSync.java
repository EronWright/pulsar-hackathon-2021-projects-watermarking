package io.hackathon.producer;

import io.hackathon.config.AppConfig;
import io.hackathon.models.StationSensorReading;
import io.hackathon.utils.AdminUtils;
import io.hackathon.utils.AppUtils;
import io.hackathon.utils.ClientUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MultipleProducersPartitionedSync {
    public static void main(String[] args) throws IOException {
        PulsarAdmin pulsarAdmin = AdminUtils.initAdminClient();
        AdminUtils.createPartitionedTopics(pulsarAdmin, AppConfig.partitionnedTopicName, AppConfig.numberOfPartitions);

        List<StationSensorReading> stationSensorReadings = AppUtils.loadStationSensorReadingsData();

        PulsarClient pulsarClient = ClientUtils.initPulsarClient();
        Producer<StationSensorReading> producer = ClientUtils.initPartitionedProducer(
                pulsarClient,
                AppConfig.partitionnedTopicName,
                "ssr-producer"
        );

        int totalMessages = 0;
        long t1 = System.currentTimeMillis();
        Timestamp currentWatermark = stationSensorReadings.get(0).getMeasurementTimestamp();

        for (StationSensorReading ssr : stationSensorReadings) {
            if (ssr.getMeasurementTimestamp().after(currentWatermark)) {
                currentWatermark = ssr.getMeasurementTimestamp();
            }

            MessageId messageId = producer.newMessage(JSONSchema.of(StationSensorReading.class))
                    .key(ssr.getStationName())
                    .value(ssr)
                    .send();

            System.out.printf("Sent message with id: '%s' and payload: %s%n", messageId.toString(), ssr);
            totalMessages += 1;
        }
        long t2 = System.currentTimeMillis();
        long totalTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(t2 - t1);
        producer.flush();
        System.out.printf("Total messages produced: %s in %s seconds.%n", totalMessages, totalTimeSeconds);

        System.out.println("Sending watermark: " + currentWatermark);
        WatermarkId watermarkId = producer.newWatermark()
                .eventTime(currentWatermark.getTime())
                .send();

        System.out.printf("Producer %s sent watermark %s successfully.%n", producer.getProducerName(), watermarkId.toString());

        producer.flush();

        producer.close();
        pulsarClient.close();
        pulsarAdmin.close();
    }
}
