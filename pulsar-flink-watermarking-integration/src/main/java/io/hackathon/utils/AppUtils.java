package io.hackathon.utils;

import io.hackathon.config.AppConfig;
import io.hackathon.models.StationSensorReading;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AppUtils {

    public static List<StationSensorReading> loadStationSensorReadingsData() throws IOException {
        List<StationSensorReading> stationSensorReadingStream = AppUtils
                .readData(AppConfig.inputFilePath)
                .skip(1)
                .map(AppUtils::strToStationSensorReading)
                .filter(AppUtils::hasReadings)
                .collect(Collectors.toList());

        outOfOrderEventsCount(stationSensorReadingStream);
        return stationSensorReadingStream;
    }

    public static HashMap<String, List<StationSensorReading>> groupReadingsByStation(List<StationSensorReading> stationSensorReadingStream) {
        stationSensorReadingStream.stream().map(StationSensorReading::getStationName)
                .distinct()
                .forEach(System.out::println);


        List<StationSensorReading> station1 = stationSensorReadingStream
                .stream()
                .filter(sst -> sst.getStationName().equals("Oak Street Weather Station"))
                .collect(Collectors.toList());

        List<StationSensorReading> station2 = stationSensorReadingStream
                .stream()
                .filter(sst -> sst.getStationName().equals("Foster Weather Station"))
                .collect(Collectors.toList());

        List<StationSensorReading> station3 = stationSensorReadingStream
                .stream()
                .filter(sst -> sst.getStationName().equals("63rd Street Weather Station"))
                .collect(Collectors.toList());

        HashMap<String, List<StationSensorReading>> stationData = new HashMap<String, List<StationSensorReading>>() {{
            put("station1", station1);
            put("station2", station2);
            put("station3", station3);
        }};
        stationData.forEach((k, v) -> {
            System.out.println("Computing OOE for " + k);
            outOfOrderEventsCount(v);
        });

        return stationData;
    }

    private static void outOfOrderEventsCount(List<StationSensorReading> stationSensorReadingStream) {
        int totalOutofOrderEvents = 0;
        for (int i = 1; i < stationSensorReadingStream.size(); i ++) {
            if (stationSensorReadingStream.get(i - 1).getMeasurementTimestamp().after(stationSensorReadingStream.get(i).getMeasurementTimestamp())) {
                totalOutofOrderEvents += 1;
            }
        }

        System.out.println("Total Events: " + stationSensorReadingStream.size());
        System.out.println("Total Out of order events: " + totalOutofOrderEvents);
    }

    private static Stream<String> readData(String inputPath) throws IOException {
        Path path = Paths.get(inputPath);
        return Files.lines(path);
    }

    private static StationSensorReading strToStationSensorReading(String line) {
        String[] tokens = line.split(",");

        if (tokens[1].equals("") || tokens[16].equals("")) {
            return new StationSensorReading();
        }

        String stationName = tokens[0];
        Timestamp measurementTimestamp = timestampParser(tokens[1]);
        Double airTemperature = strToDoubleParser(tokens[2]);
        Double wetBulbTemperature = strToDoubleParser(tokens[3]);
        Double humidity = strToDoubleParser(tokens[4]);
        Double rainIntensity = strToDoubleParser(tokens[5]);
        Double intervalRain = strToDoubleParser(tokens[6]);
        Double totalRain = strToDoubleParser(tokens[7]);
        Double precipitationType = strToDoubleParser(tokens[8]);
        Double windDirection = strToDoubleParser(tokens[9]);
        Double windSpeed = strToDoubleParser(tokens[10]);
        Double maximumWindSpeed = strToDoubleParser(tokens[11]);
        Double barometricPressure = strToDoubleParser(tokens[12]);
        Double solarRadiation = strToDoubleParser(tokens[13]);
        Double heading = strToDoubleParser(tokens[14]);
        Double batteryLife = strToDoubleParser(tokens[15]);
        Timestamp measurementTimestampLabel = timestampParser(tokens[16]);
        String measurementID = tokens[17];

        return new StationSensorReading(stationName,
                measurementTimestamp,
                airTemperature,
                wetBulbTemperature,
                humidity,
                rainIntensity,
                intervalRain,
                totalRain,
                precipitationType,
                windDirection,
                windSpeed,
                maximumWindSpeed,
                barometricPressure,
                solarRadiation,
                heading,
                batteryLife,
                measurementTimestampLabel,
                measurementID);
    }

    private static boolean hasReadings(StationSensorReading stationSensorReading) {
        if (stationSensorReading.getMeasurementTimestamp() != null) {
            return true;
        } return false;
    }

    private static Double strToDoubleParser(String str) {
        try {
            return Double.parseDouble(str);
        } catch (NumberFormatException ex) {
            return Double.NaN;
        }
    }

    private static Timestamp timestampParser(String inputTimestamp) {
        DateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aa");
        DateFormat inputFormat2 = new SimpleDateFormat("MM/dd/yyyy hh:mm aa");
        DateFormat outputformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        String output = null;

        // try to parse with the first format
        Date parsedDate = parseWithFormatter(inputTimestamp, inputFormat, inputFormat2);
        output = outputformat.format(parsedDate);
        return Timestamp.valueOf(output);
    }

    private static Date parseWithFormatter(String timestamp, DateFormat dateFormat1, DateFormat dateFormat2) {
        Date date = null;
        try {
            // try to parse with the first format
            return dateFormat1.parse(timestamp);
        } catch (ParseException e) {
            // if it fails try the second format
            try {
                return dateFormat2.parse(timestamp);
            } catch (ParseException parseException) {
                e.printStackTrace();
                return date;
            }
        }
    }
}
