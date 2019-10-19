package com.github.fancyideas.mapreduce.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class MyRunner implements Tool {

    private Configuration configuration;

    public MyRunner(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public int run(String[] strings) throws Exception {
        String inputPath = strings[0];
        String sequenceOutPutPath = strings[1];
        Job job = null;
        try {
            job = Job.getInstance(configuration, "combine small files to sequencefile");
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setJarByClass(MyInputFormatMain.class);
        //设置自定义输入格式
        job.setInputFormatClass(MyInputFormat.class);
        MyInputFormat.addInputPath(job, new Path(inputPath));
        //设置输出格式SequenceFileOutputFormat及输出路径
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(sequenceOutPutPath));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setMapperClass(SequenceFileMapper.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }
}
