package io.hackathon.utils;

import io.hackathon.config.AppConfig;
import io.hackathon.interceptors.CustomMsgListener;
import io.hackathon.models.StationSensorReading;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClientUtils {

    public static PulsarClient initPulsarClient() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(AppConfig.SERVICE_URL)
                .build();
    }

    public static Producer<StationSensorReading> initSimpleProducer(final PulsarClient pulsarClient,
                                                                    String topicName,
                                                                    String producerName) throws PulsarClientException {
        return pulsarClient.newProducer(JSONSchema.of(StationSensorReading.class))
                .topic(topicName)
                .producerName(producerName)
                .create();
    }

    public static Producer<StationSensorReading> initPartitionedProducer(final PulsarClient pulsarClient,
                                                                    String topicName,
                                                                    String producerName) throws PulsarClientException {
        return pulsarClient.newProducer(JSONSchema.of(StationSensorReading.class))
                .topic(topicName)
                .producerName(producerName)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .hashingScheme(HashingScheme.Murmur3_32Hash)
                .messageRouter(new MessageRouter() {
                    @Override
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        String key = msg.getKey();
                        if (key.equals("Oak Street Weather Station")) {
                            return 0;
                        } else if (key.equals("Foster Weather Station")) {
                            return 1;
                        } else {
                            return 2;
                        }
                    }
                })
                .create();
    }

    public static void doProducerWork(final Producer<StationSensorReading> producer,
                                      final List<StationSensorReading> stationSensorReadings) throws PulsarClientException {
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
    }

    public static Consumer<StationSensorReading> initSimpleConsumer(final PulsarClient pulsarClient,
                                                      String topicName,
                                                      String subscriptionName) throws PulsarClientException {
//        HashMap<String, Object> conf = new HashMap<String, Object>() {{
//            put("watermarkingEnabled", true);
//        }};

        return pulsarClient.newConsumer(JSONSchema.of(StationSensorReading.class))
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .consumerName("watermarking-consumer")
                .messageListener(new CustomMsgListener())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
    }
}
