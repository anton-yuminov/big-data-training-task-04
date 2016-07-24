package com.epam.training.bigdata.map;

import com.epam.training.bigdata.model.MapResultDTO;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Map<S, T extends BinaryComparable> extends Mapper<LongWritable, Text, IntWritable, MapResultDTO> {

    private static final Logger logger = LoggerFactory.getLogger(Map.class); // В промышленном варианте я бы ушел в сторону счетчиков в большинстве случаев

    protected String[] extractLineValue(String line) {
        return line.split("\\t");
    }

    protected Integer extractCityId(String[] lineValues) {
        Integer result = null;
        String s = lineValues[7];
        try {
            result = Integer.valueOf(s);
        } catch (Exception e) {
            logger.warn("Can't parse city {} from input line: {}", s, lineValues, e);
        }
        return result;
    }

    protected String extractOs(String[] lineValues) {
        String result = null;
        String userAgentStr = lineValues[4];
        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentStr);
        OperatingSystem operatingSystem = userAgent.getOperatingSystem();
        if (operatingSystem != null) {
            result = operatingSystem.getName();
        }
        return result;
    }

    protected Integer extractPrice(String[] lineValues) {
        Integer result = 0;
        String s = lineValues[20];
        try {
            result = Integer.valueOf(s);
        } catch (Exception e) {
            logger.warn("Can't parse price {} from input line: {}", s, lineValues, e);
        }
        return result;
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] lineValues = extractLineValue(line);
        Integer cityId = extractCityId(lineValues);

        String os = extractOs(lineValues);
        if (os == null) {
            logger.warn("OS is null for line: {}", line);
            os = "";
        }
        MapResultDTO result = new MapResultDTO(os, 1);

        Integer price = extractPrice(lineValues);
        if (price == null) {
            logger.warn("Price is null for line {}", line);
        } else {
            if (price >= 250) {
                context.write(new IntWritable(cityId), result);
            }
        }
    }
}
