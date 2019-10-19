package com.github.fancyideas.mapreduce.mrcompress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CountSearchMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text textOut = new Text();
    private IntWritable intWritableOut = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordArray = getWordArray(value.toString());
        textOut.set(wordArray[1]);
        context.write(textOut, intWritableOut);
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
