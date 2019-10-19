package com.github.fancyideas.mapreduce.cleandata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CleanMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    private NullWritable nullWritable = NullWritable.get();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("dataCleaning", "damagedRecord");
        String[] wordArray = getWordArray(value.toString());
        if (wordArray.length != 6) {
            counter.increment(1L);
        } else {
            context.write(value, nullWritable);
        }
    }

    private String[] getWordArray(String value) {
        StringTokenizer contentIterator = new StringTokenizer(value, "\t");
        String[] wordArray = new String[contentIterator.countTokens()];
        int wordIndex = 0;
        while (contentIterator.hasMoreElements()) {
            String currentWord = contentIterator.nextElement().toString();
            wordArray[wordIndex] = currentWord;
            wordIndex++;
        }
        return wordArray;
    }

}
