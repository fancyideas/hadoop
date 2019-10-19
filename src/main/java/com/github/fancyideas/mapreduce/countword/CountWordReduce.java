package com.github.fancyideas.mapreduce.countword;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountWordReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int everyWordCount = 0;
        while (values.iterator().hasNext()) {
            everyWordCount += values.iterator().next().get();
        }
        context.write(key, new IntWritable(everyWordCount));
    }
}
