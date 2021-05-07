package io.hackathon.producer;

import io.hackathon.config.AppConfig;
import io.hackathon.models.StationSensorReading;
import io.hackathon.utils.AppUtils;
import io.hackathon.utils.ClientUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;


import java.io.IOException;
import java.util.List;

public class SyncProducer {
    public static void main(String[] args) throws IOException {
        List<StationSensorReading> stationSensorReadings = AppUtils.loadStationSensorReadingsData();

        PulsarClient pulsarClient = ClientUtils.initPulsarClient();
        Producer<StationSensorReading> producer = ClientUtils.initSimpleProducer(
                pulsarClient,
                AppConfig.topicNameSingle,
                "ssr-producer"
        );

        ClientUtils.doProducerWork(producer, stationSensorReadings);

        producer.close();
        pulsarClient.close();
    }
}
