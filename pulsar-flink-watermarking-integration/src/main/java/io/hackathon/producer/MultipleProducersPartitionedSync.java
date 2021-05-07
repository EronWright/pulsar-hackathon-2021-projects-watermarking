package io.hackathon.producer;

import io.hackathon.config.AppConfig;
import io.hackathon.models.StationSensorReading;
import io.hackathon.utils.AdminUtils;
import io.hackathon.utils.AppUtils;
import io.hackathon.utils.ClientUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;

import java.io.IOException;
import java.util.List;

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

        ClientUtils.doProducerWork(producer, stationSensorReadings);

        producer.close();
        pulsarClient.close();
        pulsarAdmin.close();
    }
}
