import com.epam.training.bigdata.map.Map;
import com.epam.training.bigdata.model.MapResultDTO;
import com.epam.training.bigdata.partitioner.OsPartitioner;
import com.epam.training.bigdata.reduce.Reduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BidsMR extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.set("mapred.output.compress", "true");
//        conf.set("mapred.compress.map.output", "true");

        //conf.setClass("mapred.output.compression.codec", SnappyCodec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf);
        job.setJobName("Bids job");
        job.setJarByClass(BidsMR.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MapResultDTO.class);

        job.setMapperClass(Map.class);
//        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(3);

        job.setPartitionerClass(OsPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.addCacheFile(URI.create(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

//    public static class Combine extends Reducer<Text, BidsMR.IpInfo, Text, BidsMR.IpInfo> {
//
//        public void reduce(Text key, Iterable<BidsMR.IpInfo> values, Context context)
//                throws IOException, InterruptedException {
//            long amount = 0;
//            int count = 0;
//            for (BidsMR.IpInfo ipInfo : values) {
//                count += ipInfo.requestsCount.get();
//                amount += ipInfo.totalBytes.get();
//            }
//            BidsMR.IpInfo result = new BidsMR.IpInfo(amount, count);
//            context.write(key, result);
//        }
//    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BidsMR(), args);
        System.exit(res);
    }

}