package com.github.fancyideas.mapreduce.customcombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CustomCombinerMain {
    public static void main(String[] args) throws Exception {
        final String MUST_BE = "必须输入两个参数！[0]:源路径，[1]:目标路径";
        assert args != null : MUST_BE;
        assert args.length == 2 : MUST_BE;
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, CustomCombinerMain.class.getSimpleName());
        job.setJarByClass(CustomCombinerMain.class);
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(CountWordMap.class);
        job.setCombinerClass(CountWordReduce.class);
        job.setReducerClass(CountWordReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置combinner combiner = reduce 所以类一样
        job.setCombinerClass(CountWordReduce.class);
        // 自定义溢写分区器的必要设置
        job.setPartitionerClass(MyCombiner.class);
        // 自定义溢写分区器的必要设置
        job.setNumReduceTasks(4);
        job.waitForCompletion(true);
    }
}
