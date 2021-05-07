package io.hackathon.config;

public class AppConfig {
    public static final String inputFilePath = "pulsar-flink-watermarking-integration/src/main/resources/beach-weather-stations-automated-sensors-1.csv";
    public static final String ADMIN_URL = "http://localhost:8080";
    public static final String SERVICE_URL ="pulsar://localhost:6650";

    public static final String topicNameSingle = "watermarks-test-single";

    public static final String partitionnedTopicName = "watermarks-test-partitioned";
    public static final int numberOfPartitions = 3;

    public static final String TRANSACTIONS_TENANT = "pulsar";
    public static final String TRANSACTIONS_NAMESPACE = "system";

}
