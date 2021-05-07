package io.hackathon.config;

public class AppConfig {
    public static final String inputFilePath = "pulsar-flink-watermarking-integration/src/main/resources/beach-weather-stations-automated-sensors-1.csv";
    public static final String SERVICE_URL ="pulsar://localhost:6650";

    public static final String partitionnedTopicName = "watermarks-test";
    public static final int numberOfPartitions = 5;
}
