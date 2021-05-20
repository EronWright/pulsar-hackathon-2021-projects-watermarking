package io.hackathon.consumers;

import com.google.devtools.common.options.*;
import io.hackathon.config.AppConfig;
import io.hackathon.models.StationSensorReading;
import io.hackathon.utils.ClientUtils;
import org.apache.pulsar.client.api.*;

public class PulsarConsumer {

    public static void main(String[] args) throws PulsarClientException {
        OptionsParser parser = OptionsParser.newOptionsParser(PulsarConsumerOptions.class);
        parser.parseAndExitUponError(args);
        PulsarConsumerOptions options = parser.getOptions(PulsarConsumerOptions.class);
        assert options != null;

        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(AppConfig.SERVICE_URL)
                .build();

        Consumer<StationSensorReading> consumer = ClientUtils.initSimpleConsumer(
                pulsarClient,
                options.topicName,
                options.subscriptionName,
                options.consumerName,
                !options.faultNoack
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
