package com.github.fancyideas.mapreduce.mrcompress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CountSearchCompressMain {
    public static void main(String[] args) throws Exception {
        final String MUST_BE = "必须输入两个参数！[0]:源路径，[1]:目标路径";
        assert args != null : MUST_BE;
        assert args.length == 2 : MUST_BE;
        Configuration configuration = new Configuration();
        // 压缩的必要设置
        //开启map输出进行压缩的功能
        configuration.set("mapreduce.map.output.compress", "true");
        configuration.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        configuration.set("mapreduce.output.fileoutputformat.compress", "true");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        // end
        Job job = Job.getInstance(configuration, CountSearchCompressMain.class.getSimpleName());
        job.setJarByClass(CountSearchCompressMain.class);
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(CountSearchMap.class);
        job.setCombinerClass(CountSearchReduce.class);
        job.setReducerClass(CountSearchReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);
    }
}
