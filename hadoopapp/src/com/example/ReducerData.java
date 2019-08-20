package com.example;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerData extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    FloatWritable maxValue = new FloatWritable();


    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float maxPercentValue=0;
        float temp_val=0;

        for (FloatWritable value : values) {
            temp_val = value.get();
            if (temp_val > maxPercentValue) {
                maxPercentValue = temp_val;
            }
        }
        maxValue.set(maxPercentValue);

        context.write(key, maxValue);
    }
}
