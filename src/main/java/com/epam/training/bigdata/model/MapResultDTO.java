package com.epam.training.bigdata.model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapResultDTO implements WritableComparable<MapResultDTO> {

    private Text os;
    private IntWritable count;

    public MapResultDTO() {
        os = new Text();
        count = new IntWritable();
    }

    public MapResultDTO(String os, int count) {
        this.os = new Text(os);
        this.count = new IntWritable(count);
    }

    @Override
    public int compareTo(MapResultDTO o) {
        int result = this.count.compareTo(o.count);
        if (result == 0) {
            result = this.os.compareTo(o.os);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        os.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        os.readFields(dataInput);
        count.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MapResultDTO that = (MapResultDTO) o;

        if (!os.equals(that.os)) return false;
        return count.equals(that.count);

    }

    @Override
    public int hashCode() {
        int result = os.hashCode();
        result = 31 * result + count.hashCode();
        return result;
    }

    public Text getOs() {
        return os;
    }

    public IntWritable getCount() {
        return count;
    }
}
