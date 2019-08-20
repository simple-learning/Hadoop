package com.example;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperData extends Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        for (String word : line.split("\\n")) {
            String[] tokens = word.split(",");
            float stock_per = Float.parseFloat(tokens[8]);
            context.write(new Text(tokens[1]), new FloatWritable(stock_per));
        }
    }
}
