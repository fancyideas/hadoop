package com.github.fancyideas.mapreduce.cleandata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CleanMain {
    public static void main(String[] args) throws Exception {
        final String MUST_BE = "必须输入两个参数！[0]:源路径，[1]:目标路径";
        assert args != null : MUST_BE;
        assert args.length == 2 : MUST_BE;
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, CleanMain.class.getSimpleName());
        job.setJarByClass(CleanMain.class);
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(CleanMap.class);
//        job.setCombinerClass(CountWordReduce.class);
//        job.setReducerClass(CountWordReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
    }
}
