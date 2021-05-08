package io.hackathon;

import io.hackathon.config.AppConfig;
import org.apache.pulsar.client.api.*;

import java.io.IOException;


public class SensorReadingProducer {

    public static void main(String[] args) throws IOException {
//        List<StationSensorReading> stationSensorReadingStream = UtilsHelper
//                .readData(AppConfig.inputFilePath)
//                .skip(1)
//                .map(UtilsHelper::strToStationSensorReading)
//                .filter(UtilsHelper::hasReadings)
//                .collect(Collectors.toList());
//        stationSensorReadingStream.forEach(System.out::println);
//        System.out.println(stationSensorReadingStream.size());

        // create a pulsar client
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(AppConfig.SERVICE_URL)
                .build();


//        CustomProducerInterceptor interceptor = new CustomProducerInterceptor();
//        Producer<StationSensorReading> producer = pulsarClient
//                .newProducer(JSONSchema.of(StationSensorReading.class))
//                .topic(AppConfig.partitionnedTopicName)
//                .enableBatching(true)
////                .blockIfQueueFull(true)
////                .maxPendingMessages(10000)
////                .intercept(interceptor)
//                .create();

        // load messages and produce them to pulsar
        long t1 = System.currentTimeMillis();

//        Stream<CompletableFuture<MessageId>> cfStream = stationSensorReadingStream
//                .stream()
//                .map(stationSensorReading -> {
//
//                    CompletableFuture<MessageId> messageIdCompletableFuture = producer
//                            .newMessage()
//                            .key(stationSensorReading.getStationName())
//                            .value(stationSensorReading)
//                            .eventTime(stationSensorReading.getMeasurementTimestamp().getTime())
//                            .sendAsync();
////                    producer.flushAsync();
//                    return messageIdCompletableFuture;
//                });

//        producer.newWatermark().eventTime().send();
//        producer.flush();
//        cfStream.collect(Collectors.toList()).forEach(CompletableFuture::join);

//        Timestamp currentWatermark = stationSensorReadingStream.get(0).getMeasurementTimestamp();
//
//        for (StationSensorReading ssr : stationSensorReadingStream) {
//            if (ssr.getMeasurementTimestamp().after(currentWatermark)) {
//                currentWatermark = ssr.getMeasurementTimestamp();
//            }
//
//            System.out.println("Sending: " + ssr);
//            producer
//                    .newMessage()
//                    .key(ssr.getStationName())
//                    .value(ssr)
//                    .eventTime(ssr.getMeasurementTimestamp().getTime())
//                    .send();
//        }
//
//
//        long t2 = System.currentTimeMillis();
//        long totalTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(t2 - t1);
//
//        System.out.printf("Total messages produced: %s in %s seconds.%n", 0, totalTimeSeconds);
//
//        System.out.println("Done sending messages -> sending watermark with timestamp: " + currentWatermark);
//
//        producer.newWatermark()
//                .eventTime(currentWatermark.getTime())
//                .send();
//
//        WatermarkId watermarkId = producer.newWatermark()
//                .eventTime(currentWatermark.getTime())
//                .send();
//        System.out.println("Published watermark: " + watermarkId);
//
//        producer.close();
//        pulsarClient.close();
    }
}
