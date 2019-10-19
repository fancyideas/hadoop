package com.github.fancyideas.mapreduce.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MyInputFormatMain {

    public static void main(String[] args) throws Exception {
        MyRunner myRunner = new MyRunner(new Configuration());
        int exitCode = ToolRunner.run(myRunner, args);
        System.exit(exitCode);
    }
}
