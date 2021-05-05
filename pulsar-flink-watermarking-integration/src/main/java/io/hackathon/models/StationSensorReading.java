package io.hackathon.models;

import java.sql.Timestamp;

public class StationSensorReading {

    private String stationName;
    private Timestamp measurementTimestamp;
    private Double airTemperature;
    private Double wetBulbTemperature;
    private Double humidity;
    private Double rainIntensity;
    private Double intervalRain;
    private Double totalRain;
    private Double precipitationType;
    private Double windDirection;
    private Double windSpeed;
    private Double maximumWindSpeed;
    private Double barometricPressure;
    private Double solarRadiation;
    private Double heading;
    private Double batteryLife;
    private Timestamp measurementTimestampLabel;
    private String measurementID;

    public StationSensorReading() {}

    public StationSensorReading(String stationName,
                                Timestamp measurementTimestamp,
                                Double airTemperature,
                                Double wetBulbTemperature,
                                Double humidity,
                                Double rainIntensity,
                                Double intervalRain,
                                Double totalRain,
                                Double precipitationType,
                                Double windDirection,
                                Double windSpeed,
                                Double maximumWindSpeed,
                                Double barometricPressure,
                                Double solarRadiation,
                                Double heading,
                                Double batteryLife,
                                Timestamp measurementTimestampLabel,
                                String measurementID) {
        this.stationName = stationName;
        this.measurementTimestamp = measurementTimestamp;
        this.airTemperature = airTemperature;
        this.wetBulbTemperature = wetBulbTemperature;
        this.humidity = humidity;
        this.rainIntensity = rainIntensity;
        this.intervalRain = intervalRain;
        this.totalRain = totalRain;
        this.precipitationType = precipitationType;
        this.windDirection = windDirection;
        this.windSpeed = windSpeed;
        this.maximumWindSpeed = maximumWindSpeed;
        this.barometricPressure = barometricPressure;
        this.solarRadiation = solarRadiation;
        this.heading = heading;
        this.batteryLife = batteryLife;
        this.measurementTimestampLabel = measurementTimestampLabel;
        this.measurementID = measurementID;
    }

    public String getStationName() {
        return stationName;
    }

    public Timestamp getMeasurementTimestamp() {
        return measurementTimestamp;
    }

    public Double getAirTemperature() {
        return airTemperature;
    }

    public Double getWetBulbTemperature() {
        return wetBulbTemperature;
    }

    public Double getHumidity() {
        return humidity;
    }

    public Double getRainIntensity() {
        return rainIntensity;
    }

    public Double getIntervalRain() {
        return intervalRain;
    }

    public Double getTotalRain() {
        return totalRain;
    }

    public Double getPrecipitationType() {
        return precipitationType;
    }

    public Double getWindDirection() {
        return windDirection;
    }

    public Double getWindSpeed() {
        return windSpeed;
    }

    public Double getMaximumWindSpeed() {
        return maximumWindSpeed;
    }

    public Double getBarometricPressure() {
        return barometricPressure;
    }

    public Double getSolarRadiation() {
        return solarRadiation;
    }

    public Double getHeading() {
        return heading;
    }

    public Double getBatteryLife() {
        return batteryLife;
    }

    public Timestamp getMeasurementTimestampLabel() {
        return measurementTimestampLabel;
    }

    public String getMeasurementID() {
        return measurementID;
    }

    @Override
    public String toString() {
        return "StationSensorReadings{" +
                "stationName='" + stationName + '\'' +
                ", measurementTimestamp=" + measurementTimestamp +
                ", airTemperature=" + airTemperature +
                ", wetBulbTemperature=" + wetBulbTemperature +
                ", humidity=" + humidity +
                ", rainIntensity=" + rainIntensity +
                ", intervalRain=" + intervalRain +
                ", totalRain=" + totalRain +
                ", precipitationType=" + precipitationType +
                ", windDirection=" + windDirection +
                ", windSpeed=" + windSpeed +
                ", maximumWindSpeed=" + maximumWindSpeed +
                ", barometricPressure=" + barometricPressure +
                ", solarRadiation=" + solarRadiation +
                ", heading=" + heading +
                ", batteryLife=" + batteryLife +
                ", measurementTimestampLabel=" + measurementTimestampLabel +
                ", measurementID='" + measurementID + '\'' +
                '}';
    }
}
