package com.github.fancyideas.mapreduce.customcombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyCombiner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        switch (text.toString()) {
            case "Bear":
                return 0;
            case "Dear":
                return 1;
            case "Car":
                return 2;
            default:
                return 3;
        }
    }
}
