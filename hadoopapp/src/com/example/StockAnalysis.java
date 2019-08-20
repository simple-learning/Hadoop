package com.example;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockAnalysis {
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage: StockPercentChangeDriver <input dir> <output dir>\n");
            System.exit(-1);
        }


        Configuration conf = new Configuration();
        //conf.set("","")
        Job job = Job.getInstance(conf, "NYSE_Computation");

        job.setJarByClass(StockAnalysis.class);

        //job.setJobName("NYSE Stock");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MapperData.class);
        job.setCombinerClass(ReducerData.class);
        job.setReducerClass(ReducerData.class);
        job.setNumReduceTasks(10);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
