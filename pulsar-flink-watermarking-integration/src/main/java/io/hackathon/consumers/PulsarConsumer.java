package io.hackathon.consumers;

import io.hackathon.config.AppConfig;
import io.hackathon.models.StationSensorReading;
import io.hackathon.utils.ClientUtils;
import org.apache.pulsar.client.api.*;

public class PulsarConsumer {

    public static void main(String[] args) throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(AppConfig.SERVICE_URL)
                .build();

        Consumer<StationSensorReading> consumer = ClientUtils.initSimpleConsumer(
                pulsarClient,
                AppConfig.topicNameSingle,
                "watermarking-subs-test"
        );

        // add a shutdown hook to clear the resources
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> {
                    System.out.println("Closing consumer and pulsar client..");
                    try {
                        consumer.close();
                        pulsarClient.close();
                    } catch (PulsarClientException e) {
                        e.printStackTrace();
                    }
                }));
    }
}
