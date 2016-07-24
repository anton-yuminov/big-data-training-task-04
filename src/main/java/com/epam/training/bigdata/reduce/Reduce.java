package com.epam.training.bigdata.reduce;

import com.epam.training.bigdata.model.MapResultDTO;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.InputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.file.tfile.Utils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reduce extends Reducer<IntWritable, MapResultDTO, Text, IntWritable> {

    private static final Logger logger = LoggerFactory.getLogger(Reduce.class);

    protected Properties cities = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        initCities(context);
    }

    protected void initCities(Context context) {

        logger.info("Starting extraction of cities...");
        try {
            URI[] cacheFiles = context.getCacheFiles();
            logger.info("Found cache files: {}", cacheFiles);
            for (URI uri : cacheFiles) {
                if (uri.getPath().contains("city.en.properties")) {
                    logger.info("Found cache file with cities: {}", uri);
                    cities = new Properties();
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    try (InputStream in = fs.open(new Path(uri))) {
                        cities.load(in);
                    }
                    logger.info("Cities loaded: {}", cities);
                }
            }
        } catch (Exception e) {
            logger.warn("Error while init Reduce: {}", e.getMessage(), e);
        }
    }

    @Override
    public void reduce(IntWritable key, Iterable<MapResultDTO> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        for (MapResultDTO r : values) {
            count += r.getCount().get();
        }
        String city = null;
        if (cities != null) {
            city = cities.getProperty(String.valueOf(key.get()));
        }
        if (city == null) {
            city = String.valueOf(key.get());
        }
        context.write(new Text(city), new IntWritable(count));
    }
}
