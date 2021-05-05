package io.hackathon;

import io.hackathon.config.AppConfig;
import io.hackathon.models.StationSensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonDeser;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;

import java.util.Properties;

public class PulsarFlinkConsumer {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setRestartStrategy(RestartStrategies.noRestart());

        Properties props = new Properties();
        props.setProperty("topic", AppConfig.partitionnedTopicName);
        props.setProperty("scan.startup.mode", "earliest");
        props.setProperty("partition.discovery.interval-millis", "5000");

        PulsarDeserializationSchema<StationSensorReading> pulsarDeserializationSchema =
                PulsarDeserializationSchema.valueOnly(JsonDeser.of(StationSensorReading.class));

        FlinkPulsarSource<StationSensorReading> source = new FlinkPulsarSource<>(
                AppConfig.SERVICE_URL,
                AppConfig.ADMIN_URL,
                pulsarDeserializationSchema,
                props);

        // or setStartFromLatest、setStartFromSpecificOffsets、setStartFromSubscription
        source.setStartFromEarliest();

        DataStream<StationSensorReading> stream = env.addSource(source);

        stream.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
