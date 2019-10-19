package com.github.fancyideas.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

public class ReadFileFromHDFS {

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystem.get(URI.create(args[0]), new Configuration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(args[0]));
        IOUtils.copyBytes(fsDataInputStream, System.out, 4096, true);
    }
}
