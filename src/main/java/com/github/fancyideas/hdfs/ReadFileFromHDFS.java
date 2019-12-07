package com.github.fancyideas.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

public class ReadFileFromHDFS {

    public static void main(String[] args) throws Exception {
        String destination = "hdfs://node01:8020/test.txt";
        FileSystem fileSystem = FileSystem.get(URI.create(destination), new Configuration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(destination));
        IOUtils.copyBytes(fsDataInputStream, System.out, 4096, true);
    }
}
