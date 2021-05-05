package io.hackathon;

import io.hackathon.config.AppConfig;
import io.hackathon.interceptors.CustomProducerInterceptor;
import io.hackathon.models.StationSensorReading;
import io.hackathon.utils.UtilsHelper;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class SensorReadingProducer {

    public static void main(String[] args) throws IOException {
        List<StationSensorReading> stationSensorReadingStream = UtilsHelper
                .readData(AppConfig.inputFilePath)
                .skip(1)
                .map(UtilsHelper::strToStationSensorReading)
                .filter(UtilsHelper::hasReadings)
                .collect(Collectors.toList());
        stationSensorReadingStream.forEach(System.out::println);
        System.out.println(stationSensorReadingStream.size());

        // create a pulsar client
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(AppConfig.SERVICE_URL)
                .build();


        CustomProducerInterceptor interceptor = new CustomProducerInterceptor();
        Producer<StationSensorReading> producer = pulsarClient
                .newProducer(JSONSchema.of(StationSensorReading.class))
                .topic(AppConfig.partitionnedTopicName)
                .blockIfQueueFull(true)
                .maxPendingMessages(10000)
                .intercept(interceptor)
                .create();

        // load messages and produce them to pulsar
        long t1 = System.currentTimeMillis();
        Stream<CompletableFuture<MessageId>> cfStream = stationSensorReadingStream
                .stream()
                .map(stationSensorReading -> {
                    CompletableFuture<MessageId> messageIdCompletableFuture = producer
                            .newMessage()
                            .key(stationSensorReading.getStationName())
                            .value(stationSensorReading)
                            .eventTime(stationSensorReading.getMeasurementTimestamp().getTime())
                            .sendAsync();
                    producer.flushAsync();
                    return messageIdCompletableFuture;
                });

        cfStream.collect(Collectors.toList()).forEach(CompletableFuture::join);
        long t2 = System.currentTimeMillis();
        long totalTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(t2 - t1);

        System.out.printf("Total messages produced: %s in %s seconds.%n", interceptor.totalMessageCount(), totalTimeSeconds);
        // close resources
        producer.close();
        pulsarClient.close();
    }
}
