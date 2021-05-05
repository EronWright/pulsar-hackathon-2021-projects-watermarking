package io.hackathon;

import io.hackathon.config.AppConfig;
import io.hackathon.models.StationSensorReading;
import io.hackathon.utils.UtilsHelper;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;


public class PulsarFlinkTest {

    public static void main(String[] args) throws IOException {
        List<StationSensorReading> stationSensorReadingStream = UtilsHelper
                .readData(AppConfig.inputFilePath)
                .skip(1)
                .map(UtilsHelper::strToStationSensorReading)
                .filter(UtilsHelper::hasReadings)
                .collect(Collectors.toList());
        stationSensorReadingStream.forEach(System.out::println);
        System.out.println(stationSensorReadingStream.size());
    }
}
