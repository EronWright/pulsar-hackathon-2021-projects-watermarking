package io.hackathon.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.Stream;

public class UtilsHelper {
    public static Stream<String> readData(String inputPath) throws IOException {
        Path path = Paths.get(inputPath);
        return Files.lines(path);
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
