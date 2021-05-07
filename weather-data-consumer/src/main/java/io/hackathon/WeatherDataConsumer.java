package io.hackathon;

import io.hackathon.config.AppConfig;
import io.hackathon.utils.UtilsHelper;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class WeatherDataConsumer {

    public static void main(String[] args) throws IOException {

        // create a pulsar client
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(AppConfig.SERVICE_URL)
                .build();

        Consumer consumer = pulsarClient.newConsumer()
                .topic("weather-topic")
                .subscriptionName("weather-subscription")
                .subscribe();

        System.out.println("Waiting for messages");

        while (true) {
            // Wait for a message
            Message msg = consumer.receive(1, TimeUnit.SECONDS);

            try {
                // Do something with the message
                System.out.printf("Message received: %s:%s", msg.getEventTime(), new String(msg.getData()));

                // Acknowledge the message so that it can be deleted by the message broker
                consumer.acknowledge(msg);
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(msg);
            }
        }


    }
}
