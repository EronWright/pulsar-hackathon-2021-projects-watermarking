package io.hackathon.consumers;

import io.hackathon.config.AppConfig;
import io.hackathon.models.StationSensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonDeser;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;


public class PulsarFlinkConsumer {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        env.setParallelism(2);

        Properties props = new Properties();
        props.setProperty("topic", AppConfig.topicNameSingle);
        props.setProperty("scan.startup.mode", "earliest");
        props.setProperty("partition.discovery.interval-millis", "5000");

        PulsarDeserializationSchema<StationSensorReading> deserializer = PulsarDeserializationSchema
                .valueOnly(JsonDeser.of(StationSensorReading.class));


        FlinkPulsarSource<StationSensorReading> source = new FlinkPulsarSource<>(
                AppConfig.SERVICE_URL,
                AppConfig.ADMIN_URL,
                deserializer,
                props);


        source.setStartFromEarliest();
        DataStream<StationSensorReading> sensorReadingDataStream = env.addSource(source);


        tableEnvironment.createTemporaryView("readings", sensorReadingDataStream);


        tableEnvironment
                .executeSql("DESCRIBE readings")
                .print();

        tableEnvironment
                .executeSql("SELECT f0 FROM readings")
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
