package com.github.fancyideas.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

public class UploadfileToHDFS {
    public static void main(String[] args) throws Exception {
        String source = "D:\\testdata\\test02.txt";
        String destination = "hdfs://node01:8020/test02.txt";
        InputStream in = new BufferedInputStream(new FileInputStream(source));
        FileSystem fileSystem = FileSystem.get(URI.create(destination), new Configuration());
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(destination));
        IOUtils.copyBytes(in, fsDataOutputStream, 4096, true);
    }
}
