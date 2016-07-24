package com.epam.training.bigdata.map;

import com.epam.training.bigdata.model.MapResultDTO;
import com.epam.training.bigdata.reduce.Reduce;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import static org.hamcrest.core.Is.*;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class MapTest {
    @Test
    public void extractPrice() throws Exception {
        String input = "b4791d1310887ab13162503d51696ad3\t20131019161905089\t1\tCATFtG0VfMY\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0\t183.19.52.*\t216\t226\t2\t13625cb070ffb306b425cd803c4b7ab4\t733d4fa04458005aacd0e3689639fdc5\tnull\t3533117577\t300\t250\tOtherView\tNa\t52\t7323\t277\t260\tnull\t2259\t10057,10048,10059,10079,10076,10077,10083,13866,10006,10024,10148,13776,10111,10063,10116";
        Map map = new Map();
        int price = map.extractPrice(map.extractLineValue(input));
        assertThat(price, is(260));
    }

    @Test
    public void extractOs() throws Exception {
        String input = "40d28159e587158a3a06ac5a3169727b\t20131019113608817\t1\tCB5Kau8ffbj\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.64 Safari/537.31\t14.218.25.*\t216\t233\t3\tdd4270481b753dde29898e27c7c03920\te244e13495a53cd80207661bf9c2f12a\tnull\tEnt_F_Width1\t1000\t90\tNa\tNa\t70\t7336\t294\t110\tnull\t2259\t10057,11278,13800,10059,13496,14273,10076,10077,10093,10075,10083,10129,10024,10006,10110,10031,13776,10120,10052,10145,13403,10115,10063";
        Map map = new Map();
        String os = map.extractOs(map.extractLineValue(input));
        assertThat(os, is("Windows XP"));
    }

    @Test
    public void extractCityId() throws Exception {
        String input = "bcbc973f1a93e22de83133f360759f04\t20131019134022114\t1\tCALAIF9UcIi\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)\t59.34.170.*\t216\t237\t3\t7ed515fe566938ee6cfbb6ebb7ea4995\tea4e49e1a4b0edabd72386ee533de32f\tnull\tALLINONE_F_Width2\t1000\t90\tNa\tNa\t50\t7336\t294\t50\tnull\t2259\t10059,14273,10117,10075,10083,10102,10006,10148,11423,10110,10031,10126,13403,10063";
        Map map = new Map();
        Integer cityId = map.extractCityId(map.extractLineValue(input));
        assertThat(cityId, is(237));
    }

    @Test
    public void testMap() throws Exception {
        String input = "b4791d1310887ab13162503d51696ad3\t20131019161905089\t1\tCATFtG0VfMY\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0\t183.19.52.*\t216\t226\t2\t13625cb070ffb306b425cd803c4b7ab4\t733d4fa04458005aacd0e3689639fdc5\tnull\t3533117577\t300\t250\tOtherView\tNa\t52\t7323\t277\t260\tnull\t2259\t10057,10048,10059,10079,10076,10077,10083,13866,10006,10024,10148,13776,10111,10063,10116";
        Map map = new Map();
        MapDriver mapDriver = MapDriver.newMapDriver(map);
        mapDriver
                .withInput(new LongWritable(1L), new Text(input))
                .withOutput(new IntWritable(226), new MapResultDTO("Windows XP", 1))
                .runTest();
    }

    @Test
    public void testMapLowPriceEmptyOutput() throws Exception {
        String input = "710f5852a9bec40561ea85d7ff51a4e6\t20131019161902429\t1\tCATFtG0VfMY\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0\t183.19.52.*\t216\t226\t2\t13625cb070ffb306b425cd803c4b7ab4\t733d4fa04458005aacd0e3689639fdc5\tnull\t3195670606\t728\t90\tOtherView\tNa\t52\t7330\t277\t52\tnull\t2259\t10057,10048,10059,10079,10076,10077,10083,13866,10006,10024,10148,13776,10111,10063,10116";
        Map map = new Map();
        MapDriver mapDriver = MapDriver.newMapDriver(map);
        mapDriver
                .withInput(new LongWritable(1L), new Text(input))
                .withAllOutput(new ArrayList<Pair>())
                .runTest();
    }

    @Test
    public void testReduce() throws Exception {
        Reduce reduce = new Reduce();
        ReduceDriver reduceDriver = ReduceDriver.newReduceDriver(reduce);
        reduceDriver
                .withInput(new IntWritable(226), new ArrayList<MapResultDTO>() {{
                    add(new MapResultDTO("Windows XP", 2));
                    add(new MapResultDTO("Android", 6));
                }})
                .withOutput(new Text("226"), new IntWritable(8)) //TODO replace with city name
                .runTest();
    }
}