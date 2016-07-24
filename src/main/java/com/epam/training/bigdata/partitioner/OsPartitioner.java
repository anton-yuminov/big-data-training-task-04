package com.epam.training.bigdata.partitioner;

import com.epam.training.bigdata.model.MapResultDTO;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OsPartitioner extends Partitioner<IntWritable, MapResultDTO> {

    private static final Logger logger = LoggerFactory.getLogger(OsPartitioner.class);

    @Override
    public int getPartition(IntWritable intWritable, MapResultDTO mapResultDTO, int numPartitions) {
        if (numPartitions == 0) {
            logger.info("0");
            return 0;
        }
        Text os = mapResultDTO.getOs();
        if (os == null) {
            return 0;
        }
        return Math.abs(os.hashCode()) % numPartitions;
    }
}
